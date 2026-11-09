/* Classic script intentionally works on file: URLs without module loading. */
(() => {
  const file = location.protocol === 'file:';
  document.getElementById('file-entry').hidden = !file;
  if (!file) return;
  document.documentElement.dataset.ccsFileEntry = 'true';
  const credential = document.getElementById('credential');
  credential.value = '';
  credential.disabled = true;
  document.getElementById('connect-form').addEventListener('submit', event => event.preventDefault());
})();
