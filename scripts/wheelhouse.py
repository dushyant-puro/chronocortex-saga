"""Capture/verify locked wheels for this host; never download, extract or install."""
import argparse
from contextlib import contextmanager
from email.errors import MessageError
from email.parser import BytesParser
from email.policy import default
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import stat
import zipfile

from packaging.markers import default_environment
from packaging.specifiers import SpecifierSet
from packaging.tags import parse_tag, sys_tags
from packaging.utils import canonicalize_name, parse_wheel_filename

from scripts import dependency_audit as audit


MANIFEST = 'wheelhouse.json'
MAX_FILES = 256
MAX_WHEEL = 256 * 1024 * 1024
MAX_TOTAL = 1024 * 1024 * 1024
MAX_METADATA = 1024 * 1024
MAX_MANIFEST = 256 * 1024


def need(condition):
    if not condition: raise ValueError('Wheelhouse validation failed')


def context(profile,root):
    need(profile in audit.LOCKS)
    env=default_environment()
    machine=platform.machine()
    need(env['python_version']=='3.13' and env['implementation_name']=='cpython' and
         (env['sys_platform'],machine) in {('win32','AMD64'),('linux','x86_64')})
    manifest,active=audit.load_locks(root,env)
    text=(Path(root)/audit.LOCKS[profile]).read_text(encoding='utf-8')
    hashes={}
    for requirement,digests in audit.parse_lock_records(text):
        if requirement.marker is None or requirement.marker.evaluate(env):
            hashes[canonicalize_name(requirement.name)]=digests
    need(0<len(active[profile])<=MAX_FILES and set(hashes)==set(active[profile]))
    target={'python':env['python_version'],'implementation':env['implementation_name'],
            'platform':env['sys_platform'],'machine':machine}
    return {'pins':active[profile],'hashes':hashes,'target':target,'profile':profile,
            'lock_sha256':manifest['locks'][audit.LOCKS[profile]],
            'inputs_sha256':audit.digest(Path(root)/audit.MANIFEST),
            'tags':set(sys_tags()),'python_full_version':env['python_full_version']}


def regular(info):
    return (stat.S_ISREG(info.st_mode) and info.st_nlink==1 and
            not getattr(info,'st_file_attributes',0)&getattr(stat,'FILE_ATTRIBUTE_REPARSE_POINT',0))


@contextmanager
def reader(path):
    need(regular(os.lstat(path)))
    flags=os.O_RDONLY|getattr(os,'O_BINARY',0)|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0)
    with os.fdopen(os.open(path,flags),'rb') as stream:
        need(regular(os.fstat(stream.fileno())))
        yield stream


def wheel_files(directory,*,closed=False):
    directory=Path(directory)
    info=directory.lstat()
    need(stat.S_ISDIR(info.st_mode) and not directory.is_symlink() and
         not getattr(info,'st_file_attributes',0)&getattr(stat,'FILE_ATTRIBUTE_REPARSE_POINT',0))
    result=[]; count=0
    with os.scandir(directory) as entries:
        for entry in entries:
            count+=1; need(count<=MAX_FILES+1)
            if closed and entry.name==MANIFEST: continue
            if not entry.name.lower().endswith('.whl'):
                need(not closed)
                continue  # Source catalogs/other files are never copied.
            need(re.fullmatch(r'[A-Za-z0-9_.+-]+\.whl',entry.name) is not None)
            result.append(directory/entry.name)
    need(len(result)==len({p.name.casefold() for p in result}))
    return sorted(result,key=lambda p:p.name)


def identity(path,policy):
    name,version,_,tags=parse_wheel_filename(path.name)
    need(policy['pins'].get(name)==str(version) and bool(tags&policy['tags']))
    return name,str(version),tags


def inspect_wheel(path,policy):
    name,version,tags=identity(path,policy)
    with reader(path) as stream:
        before=os.fstat(stream.fileno())
        need(0<before.st_size<=MAX_WHEEL)
        digest=hashlib.sha256(); size=0
        while raw:=stream.read(1024*1024):
            size+=len(raw); need(size<=MAX_WHEEL)
            digest.update(raw)
        checksum=digest.hexdigest()
        need(size==before.st_size and checksum in policy['hashes'][name])
        stream.seek(0)
        # Unknown hashes never reach ZIP/header parsing. No archive is extracted.
        with zipfile.ZipFile(stream) as archive:
            entries=archive.infolist()
            need(0<len(entries)<=50_000)
            need(sum(e.file_size for e in entries)<=2*MAX_TOTAL)
            names=[e.filename for e in entries]
            need(len(names)==len(set(names)))
            for entry in entries:
                member=PurePosixPath(entry.filename)
                need(len(entry.filename)<=512 and '\\' not in entry.filename and
                     not member.is_absolute() and '..' not in member.parts and
                     not stat.S_ISLNK(entry.external_attr>>16) and not entry.flag_bits&1)
            metadata=[p for p in names if re.fullmatch(r'[^/]+\.dist-info/METADATA',p)]
            need(len(metadata)==1)
            parent=metadata[0].rsplit('/',1)[0]
            dist,release=parent.removesuffix('.dist-info').rsplit('-',1)
            need(canonicalize_name(dist)==name and audit.Version(release)==audit.Version(version))
            def headers(member):
                need(archive.getinfo(member).file_size<=MAX_METADATA)
                message=BytesParser(policy=default).parsebytes(archive.read(member),headersonly=True)
                need(not message.defects)
                return message
            info=headers(metadata[0]); wheel=headers(parent+'/WHEEL')
            need(len(info.get_all('Name',[]))==len(info.get_all('Version',[]))==1)
            need(canonicalize_name(str(info['Name']),validate=True)==name and audit.Version(str(info['Version']))==audit.Version(version))
            requires=info.get_all('Requires-Python',[])
            need(len(requires)<=1)
            if requires: need(SpecifierSet(str(requires[0])).contains(policy['python_full_version'],prereleases=True))
            need(wheel.get_all('Wheel-Version')==['1.0'])
            declared=set()
            for value in wheel.get_all('Tag',[]): declared.update(parse_tag(str(value)))
            need(declared==tags)
        after=os.fstat(stream.fileno())
        need((before.st_size,before.st_mtime_ns)==(after.st_size,after.st_mtime_ns))
    return {'filename':path.name,'name':name,'version':version,'bytes':size,'sha256':checksum}


def inventory(directory,policy,*,closed=False):
    files=wheel_files(directory,closed=closed)
    records=[]; seen=set(); size=0
    for path in files:
        record=inspect_wheel(path,policy)
        need(record['name'] not in seen)
        seen.add(record['name']); size+=record['bytes']; need(size<=MAX_TOTAL)
        records.append(record)
    need(seen==set(policy['pins']))
    return records


def document(policy,records):
    return {'schema':1,'profile':policy['profile'],'target':policy['target'],
            'lock_sha256':policy['lock_sha256'],'inputs_sha256':policy['inputs_sha256'],'wheels':records}


def pairs(items):
    result={}
    for key,value in items:
        need(key not in result)
        result[key]=value
    return result


def reject(_): raise ValueError('Nonfinite manifest')


def verify_archive(directory,policy):
    with reader(Path(directory)/MANIFEST) as stream:
        raw=stream.read(MAX_MANIFEST+1)
    need(len(raw)<=MAX_MANIFEST)
    data=json.loads(raw,object_pairs_hook=pairs,parse_constant=reject)
    need(type(data) is dict and type(data.get('schema')) is int and data['schema']==1)
    records=data.get('wheels')
    need(type(records) is list and 0<len(records)<=MAX_FILES)
    for record in records:
        need(type(record) is dict and set(record)=={'filename','name','version','bytes','sha256'})
        need(type(record['bytes']) is int and 0<record['bytes']<=MAX_WHEEL)
        need(all(type(record[k]) is str for k in ('filename','name','version','sha256')))
    actual=inventory(directory,policy,closed=True)
    need(data==document(policy,actual))
    return actual


def verdict(operation,profile):
    return {'schema':1,'operation':operation,'profile':profile,'passed':False,
            'installed':False,'publisher_authenticated':False,'host_reproducibility_certified':False}


ERRORS=(OSError,ValueError,TypeError,KeyError,RuntimeError,MessageError,zipfile.BadZipFile,zipfile.LargeZipFile)


def verify(directory,profile,*,root=audit.ROOT):
    report=verdict('VERIFY',profile)
    try:
        policy=context(profile,root)
        records=verify_archive(directory,policy)
        report.update(passed=True,wheel_count=len(records),total_bytes=sum(r['bytes'] for r in records),lock_sha256=policy['lock_sha256'])
    except ERRORS: report['failure']='WHEELHOUSE_UNVERIFIED'
    return report


def capture(source,output,profile,*,root=audit.ROOT):
    report=verdict('CAPTURE',profile)
    try:
        policy=context(profile,root)
        source=Path(source).absolute()
        destination=Path(output).absolute()
        need(not os.path.lexists(destination) and not destination.resolve().is_relative_to(source.resolve(strict=True)))
        records=inventory(source,policy)
        destination.mkdir(mode=0o700)  # Exclusive directory; partial failures are retained.
        for record in records:
            with reader(source/record['filename']) as incoming, (destination/record['filename']).open('xb') as outgoing:
                size=0
                while raw:=incoming.read(1024*1024):
                    size+=len(raw); need(size<=MAX_WHEEL)
                    outgoing.write(raw)
                outgoing.flush(); os.fsync(outgoing.fileno())
        need(inventory(destination,policy)==records)
        metadata=json.dumps(document(policy,records),sort_keys=True,indent=2,allow_nan=False).encode()+b'\n'
        need(len(metadata)<=MAX_MANIFEST)
        with (destination/MANIFEST).open('xb') as stream:
            stream.write(metadata); stream.flush(); os.fsync(stream.fileno())
        need(verify_archive(destination,policy)==records)
        if os.name!='nt':
            for directory in (destination,destination.parent):
                descriptor=os.open(directory,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
                try: os.fsync(descriptor)
                finally: os.close(descriptor)
        report.update(passed=True,wheel_count=len(records),total_bytes=sum(r['bytes'] for r in records),lock_sha256=policy['lock_sha256'])
    except ERRORS: report['failure']='WHEELHOUSE_UNVERIFIED'
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__,allow_abbrev=False)
    commands=parser.add_subparsers(dest='command',required=True)
    backup=commands.add_parser('capture',allow_abbrev=False)
    backup.add_argument('--source',required=True); backup.add_argument('--output',required=True)
    check=commands.add_parser('verify',allow_abbrev=False)
    check.add_argument('--directory',required=True)
    for command in (backup,check): command.add_argument('--profile',choices=tuple(audit.LOCKS),required=True)
    args=parser.parse_args()
    report=(capture(args.source,args.output,args.profile) if args.command=='capture' else verify(args.directory,args.profile))
    print(json.dumps(report,sort_keys=True,allow_nan=False))
    raise SystemExit(0 if report['passed'] else 2)


if __name__=='__main__': main()
