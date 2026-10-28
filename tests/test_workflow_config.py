"""Local parsing catches configurations that GitHub rejects before tests can run."""
from pathlib import Path

import yaml


def test_checked_in_workflows_have_scalar_executable_steps():
    paths=list((Path(__file__).resolve().parents[1]/'.github/workflows').glob('*.yml'))
    assert paths
    for path in paths:
        data=yaml.safe_load(path.read_text(encoding='utf-8'))
        assert type(data) is dict and type(data.get('jobs')) is dict and data['jobs']
        for job in data['jobs'].values():
            assert type(job) is dict and type(job.get('steps')) is list and job['steps']
            for step in job['steps']:
                assert type(step) is dict
                executable=set(step)&{'run','uses'}
                assert len(executable)==1
                command=step[next(iter(executable))]
                assert type(command) is str and command.strip()
