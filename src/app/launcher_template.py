# fiftyone-sync, Apache-2.0 license
# Filename: src/app/launcher_template.py
# Description: FiftyOne launcher HTML template for Tator dashboards.
"""FiftyOne launcher HTML template."""

LAUNCHER_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>FiftyOne Viewer – Project {{ project }}</title>
  <style>
    * { box-sizing: border-box; }
    html, body { margin: 0; min-height: 100%; font-family: system-ui, sans-serif; background: #1a1a1a; color: #e0e0e0; padding: 1rem; }
    .applet-header { padding: 0.75rem 0; font-size: 0.875rem; display: flex; align-items: center; gap: 1rem; flex-wrap: wrap; }
    .applet-header p { margin: 0; }
    .applet-header .config-yaml { margin-top: 0.5rem; font-size: 0.75rem; color: #999; white-space: pre-wrap; word-break: break-all; max-height: 6em; overflow: auto; }
    .applet-header .sync-status { font-size: 0.8rem; color: #9c9; }
    .applet-header .sync-status.error { color: #c99; }
    .applet-header button { padding: 0.35rem 0.75rem; cursor: pointer; background: #3a7bd5; color: #fff; border: none; border-radius: 4px; font-size: 0.8rem; }
    .applet-header button:hover { background: #2d6ac4; }
    .applet-header button.btn-danger { background: #b33; }
    .applet-header button.btn-danger:hover { background: #922; }
    .applet-header button:disabled { opacity: 0.6; cursor: not-allowed; }
    .applet-header .btn-icon { margin-right: 0.25rem; }
    .applet-header .btn-icon.end { margin-right: 0; margin-left: 0.25rem; }
    .applet-header select { padding: 0.35rem 0.5rem; font-size: 0.8rem; background: #2a2a2a; color: #e0e0e0; border: 1px solid #555; border-radius: 4px; min-width: 10rem; }
    .applet-header input[type="password"] { padding: 0.35rem 0.5rem; font-size: 0.8rem; background: #2a2a2a; color: #e0e0e0; border: 1px solid #555; border-radius: 4px; min-width: 12rem; }
    .applet-header a.token-link { font-size: 0.8rem; color: #6ab; }
    .applet-header a.token-link:hover { color: #8cd; text-decoration: underline; }
    .applet-header a.fiftyone-app-link { font-size: 0.8rem; color: #6ab; margin-left: 0.5rem; }
    .applet-header a.fiftyone-app-link:hover { color: #8cd; text-decoration: underline; }
    .applet-header table { border-collapse: collapse; width: 100%; max-width: 56rem; }
    .applet-header th { text-align: left; padding: 0.5rem 1rem 0.25rem 0; font-size: 0.75rem; color: #999; font-weight: 600; vertical-align: top; white-space: nowrap; width: 10rem; }
    .applet-header td { padding: 0.25rem 0; vertical-align: middle; }
    .applet-header tr + tr th { padding-top: 0.75rem; }
    .applet-header .cell-controls { display: flex; align-items: center; gap: 0.5rem 1rem; flex-wrap: wrap; }
    .applet-header .force-sync-option { display: inline-flex; align-items: center; gap: 0.35rem; font-size: 0.8rem; color: #b0b0b0; cursor: pointer; }
    .sync-log-panel { display: none; margin-top: 0.5rem; max-height: 12rem; overflow: auto; background: #0d0d0d; border: 1px solid #444; border-radius: 4px; padding: 0.5rem; font-family: ui-monospace, monospace; font-size: 0.75rem; line-height: 1.3; white-space: pre-wrap; word-break: break-all; }
    .sync-log-panel.visible { display: block; }
  </style>
</head>
<body>
  <p id="app-version" style="margin:0 0 0.5rem;font-size:0.75rem;color:#888;"></p>
  <div class="applet-header">
    <table>
      <tbody>
        <tr>
          <th>Info</th>
          <td>
            {% if message %}
            <p>{{ message }}</p>
            {% else %}
            <p>Voxel51 FiftyOne viewer – Project {{ project }} (port {{ (base_port | default(5151) | int) + ((project | int) - 1) }})</p>
            {% endif %}
            {% if config_yaml %}
            <div id="config-yaml-data" class="config-yaml" title="config_yaml" data-config="{{ config_yaml | e }}">{{ config_yaml }}</div>
            {% endif %}
          </td>
        </tr>
        {% if sync_service_url and api_url %}
        <tr>
          <th>Token</th>
          <td>
            <div class="cell-controls">
              <input type="password" id="user-token" placeholder="Your Tator API token" autocomplete="off" />
              <a href="{{ ((api_url | default('')).rstrip('/') ~ '/token') | e }}" target="_blank" rel="noopener" class="token-link">Get your token</a>
              <button type="button" id="test-token-btn">Verify Token</button>
            </div>
          </td>
        </tr>
        <tr id="vss-project-row" style="display: none;">
          <th>VSS Project</th>
          <td>
            <div class="cell-controls">
              <select id="vss-project-select" aria-label="vss-project" disabled>
                <option value="">Select VSS project</option>
              </select>
            </div>
          </td>
        </tr>
        <tr>
          <th>Tator Version</th>
          <td>
            <div class="cell-controls">
              <select id="version-select" aria-label="version" disabled>
                <option value="">Enter token and click Test</option>
              </select>
            </div>
          </td>
        </tr>
        <tr>
          <th>Section</th>
          <td>
            <div class="cell-controls">
              <select id="section-select" aria-label="section" disabled title="Optional media section filter (ANDed with query when both set).">
                <option value="">All sections</option>
              </select>
            </div>
          </td>
        </tr>
        <tr>
          <th>Box type</th>
          <td>
            <div class="cell-controls">
              <select id="box-type-select" aria-label="box-type" disabled title="Optional Tator box (localization) type filter. Only localizations of the selected box type are loaded into the dataset.">
                <option value="">All box types</option>
              </select>
            </div>
          </td>
        </tr>
        <tr>
          <th>Sync</th>
          <td>
            <div class="cell-controls">
              <button type="button" id="sync-from-tator-btn" disabled title="Loads the selected version and launches a Voxel51 (FiftyOne) viewer in another tab. If the Embedding Service is not available, the viewer will still launch but will not contain embeddings."><span class="btn-icon" aria-hidden="true">←</span>Load from Tator</button>
              <label class="force-sync-option" title="Re-fetch media and localizations from Tator instead of using cached data when available.">
                <input type="checkbox" id="force-sync-checkbox" name="force_sync" value="1"> Force sync
              </label>
              <label class="force-sync-option" title="Only include localizations whose verified attribute is set to true in the built dataset.">
                <input type="checkbox" id="verified-only-checkbox" name="verified_only" value="1"> Verified only
              </label>
              <label class="force-sync-option" title="Remove near-duplicate, blurry, dark, and low-information crops (CleanVision) from the Voxel51 dataset. Nothing is deleted in Tator.">
                <input type="checkbox" id="remove-near-duplicates-checkbox" name="remove_near_duplicates" value="1"> Remove near duplicates
              </label>
              <span id="sync-status" class="sync-status" aria-live="polite"></span>
              <a id="fiftyone-app-link" href="#" target="_blank" rel="noopener" class="fiftyone-app-link" style="display: none;">Open Voxel51</a>
            </div>
            <div id="sync-log-panel" class="sync-log-panel" aria-live="polite" title="Sync progress log"></div>
          </td>
        </tr>
        <tr id="s3-bucket-row" style="display: none;">
          <th>S3 bucket</th>
          <td>
            <div class="cell-controls">
              <input type="text" id="s3-bucket-input" placeholder="bucket (optional)" autocomplete="off" />
              <input type="text" id="s3-prefix-input" placeholder="prefix (optional)" autocomplete="off" />
            </div>
          </td>
        </tr>
        {% endif %}
        <tr>
          <th>Embedding service</th>
          <td>
            <span id="embedding-status" class="sync-status" aria-live="polite">Checking…</span>
          </td>
        </tr>
        {% if sync_service_url and api_url %}
        <tr>
          <td colspan="2" style="padding: 0.75rem 0 0.25rem;">
            <hr style="border: none; border-top: 1px solid #444; margin: 0 0 0.5rem;">
            <span style="font-size: 0.75rem; font-weight: 600; color: #999; text-transform: uppercase; letter-spacing: 0.05em;">Voxel51 Management</span>
          </td>
        </tr>
        <tr>
          <th>Voxel51 dataset</th>
          <td>
            <div class="cell-controls">
              <select id="voxel-dataset-select" aria-label="voxel-dataset" disabled title="Select the FiftyOne dataset to sync back to Tator.">
                <option value="">Enter token and click Test</option>
              </select>
              <button type="button" id="sync-to-tator-btn" disabled title="Pushes any revised data from FiftyOne back to the selected version.">Sync to Tator<span class="btn-icon end" aria-hidden="true">→</span></button>
              <button type="button" id="delete-dataset-btn" class="btn-danger" disabled title="Delete the FiftyOne dataset for the selected version. This cannot be undone. Not this only deletes the dataset from Voxel51, not Tator."><span class="btn-icon" aria-hidden="true">🗑</span>Delete Voxel51 Dataset</button>
            </div>
          </td>
        </tr>
        {% endif %}
      </tbody>
    </table>
  </div>
  <script>
    (function() {
      var project = parseInt("{{ project }}", 10) || 0;
      var iframeHost = "{{ (iframe_host | default('localhost')) }}";
      var port = {{ (port | default(5151)) | int }};
      var appUrl = 'http://' + iframeHost + ':' + port + '/';
      var syncServiceUrl = "{{ (sync_service_url | default('') | e) }}".replace(/\/$/, '');
      var projectName = "{{ (project_name | default('') | e) }}".trim();
      var databaseName = "{{ database_name | default('') | e }}" || ('fiftyone_project_' + project);
      var databaseUri = '';
      // Do not call /database-info on load: it requires valid Authorization. Resolve port/database only after token is provided and tested.
      if (project && syncServiceUrl) {
        console.log('[FiftyOne applet] database-info deferred until token is tested; initial port=', port);
      }
      var configYamlEl = document.getElementById('config-yaml-data');
      var configYaml = configYamlEl ? (configYamlEl.getAttribute('data-config') || '') : '';
      if (configYaml) window.FIFTYONE_CONFIG_YAML = configYaml;
      var apiUrl = "{{ api_url | default('') | e }}";
      var initialVersionId = "{{ version_id | default('') | e }}";
      var initialSectionId = "{{ section_id | default('') | e }}";
      var syncQuery = "{{ query | default('') | e }}";
      var syncBtn = document.getElementById('sync-from-tator-btn');
      var syncStatus = document.getElementById('sync-status');
      var fiftyoneAppLink = document.getElementById('fiftyone-app-link');
      var versionSelect = document.getElementById('version-select');
      var sectionSelect = document.getElementById('section-select');
      var boxTypeSelect = document.getElementById('box-type-select');
      var voxelDatasetSelect = document.getElementById('voxel-dataset-select');
      var vssProjectSelect = document.getElementById('vss-project-select');
      var vssProjectRow = document.getElementById('vss-project-row');
      var syncToTatorBtn = document.getElementById('sync-to-tator-btn');
      var deleteDatasetBtn = document.getElementById('delete-dataset-btn');
      var tokenInput = document.getElementById('user-token');
      var testTokenBtn = document.getElementById('test-token-btn');
      var tokenVerified = false;
      var hasDatabaseEntry = false;
      var isEnterprise = false;
      var versionId = '';
      var datasetExists = false;
      var selectedSyncDatasetName = '';
      var vssProjectKey = '';
      var vssProjectsData = [];  // full list from /vss-projects: [{key, name}]; embedding service URL is global
      var embeddingServiceReady = false;  // true when WS test passed or service not configured
      function getToken() {
        return tokenInput ? tokenInput.value.trim() : '';
      }
      function getSelectedVssProjectName() {
        // Returns the vss_project name for the selected key. No fallback; must have vss_project_key.
        if (vssProjectKey && vssProjectsData.length) {
          var found = vssProjectsData.find(function(vp) { return vp.key === vssProjectKey; });
          if (found && found.name) return found.name;
        }
        return '';
      }
      function getEmbeddingProject() {
        // Returns the project for the embedding API. Always uses vss_project_key (no fallback).
        return vssProjectKey || '';
      }
      function setVssProjectFromDropdown() {
        vssProjectKey = vssProjectSelect && vssProjectSelect.value ? vssProjectSelect.value : '';
        // Update S3 config when VSS project changes
        if (vssProjectKey && tokenVerified) {
          updateDatabaseInfo(getToken());
        }
        // Re-check embedding status for the newly selected VSS project
        checkEmbeddingService();
      }
       function setVersionFromDropdown() {
        versionId = versionSelect && versionSelect.value ? versionSelect.value : '';
        if (versionSelect) {
          var selOpt = versionSelect.options[versionSelect.selectedIndex];
          versionSelect.title = selOpt ? (selOpt.getAttribute('data-description') || '') : '';
        }
        updateSyncButtonsState();
        datasetExists = false;
        if (deleteDatasetBtn) deleteDatasetBtn.disabled = true;
        if (tokenVerified && hasDatabaseEntry && versionId) checkDatasetExists();
      }
      function setSyncDatasetFromDropdown() {
        selectedSyncDatasetName = voxelDatasetSelect && voxelDatasetSelect.value ? voxelDatasetSelect.value : '';
        if (voxelDatasetSelect) {
          var dsOpt = voxelDatasetSelect.options[voxelDatasetSelect.selectedIndex];
          voxelDatasetSelect.title = dsOpt ? dsOpt.textContent : '';
        }
        updateSyncButtonsState();
      }
      function resetSyncDatasetChoices(message) {
        if (!voxelDatasetSelect) return;
        voxelDatasetSelect.innerHTML = '';
        var opt = document.createElement('option');
        opt.value = '';
        opt.textContent = message || 'Select Voxel dataset';
        voxelDatasetSelect.appendChild(opt);
        voxelDatasetSelect.disabled = true;
        selectedSyncDatasetName = '';
      }
      function refreshSyncDatasetChoices() {
        var token = getToken();
        if (!voxelDatasetSelect) return;
        if (!syncServiceUrl || !apiUrl || !token || !tokenVerified || !hasDatabaseEntry) {
          resetSyncDatasetChoices('Select Voxel dataset');
          updateSyncButtonsState();
          return;
        }
        var currentSelection = selectedSyncDatasetName;
        voxelDatasetSelect.disabled = true;
        voxelDatasetSelect.innerHTML = '<option value="">Loading datasets…</option>';
        selectedSyncDatasetName = '';
        updateSyncButtonsState();
        var listUrl = syncServiceUrl + '/datasets?project_id=' + project + '&api_url=' + encodeURIComponent(apiUrl) + '&port=' + port;
        fetch(listUrl, {
          headers: { 'Authorization': 'Token ' + token }
        })
          .then(function(r) { return r.ok ? r.json() : null; })
          .then(function(data) {
            if (!data) return;
            var datasets = Array.isArray(data.datasets) ? data.datasets : [];
            voxelDatasetSelect.innerHTML = '';
            if (!datasets.length) {
              resetSyncDatasetChoices('No datasets found');
              updateSyncButtonsState();
              return;
            }
            datasets.forEach(function(name) {
              var opt = document.createElement('option');
              opt.value = name;
              opt.textContent = name;
              voxelDatasetSelect.appendChild(opt);
            });
            var preferred = currentSelection && datasets.indexOf(currentSelection) !== -1
              ? currentSelection
              : datasets[0];
            voxelDatasetSelect.value = preferred || '';
            voxelDatasetSelect.disabled = false;
            setSyncDatasetFromDropdown();
          })
          .catch(function() {
            resetSyncDatasetChoices('Failed to load datasets');
            updateSyncButtonsState();
          });
      }
      function updateSyncButtonsState() {
        if (syncBtn) syncBtn.disabled = !tokenVerified || !hasDatabaseEntry || !embeddingServiceReady;
        if (syncToTatorBtn) syncToTatorBtn.disabled = !tokenVerified || !hasDatabaseEntry || !versionId || !selectedSyncDatasetName;
        if (deleteDatasetBtn) deleteDatasetBtn.disabled = !tokenVerified || !hasDatabaseEntry || !versionId || !datasetExists;
      }
      function setSyncControlsEnabled(enabled) {
        tokenVerified = enabled;
        if (versionSelect) versionSelect.disabled = !enabled;
        if (sectionSelect) sectionSelect.disabled = !enabled;
        if (boxTypeSelect) boxTypeSelect.disabled = !enabled;
        if (vssProjectSelect) vssProjectSelect.disabled = !enabled;
        updateSyncButtonsState();
      }
      function checkDatasetExists() {
        var token = getToken();
        var v = versionId;
        if (!syncServiceUrl || !apiUrl || !token || !v) return;
        var existsUrl = syncServiceUrl + '/dataset-exists?project_id=' + project + '&version_id=' + encodeURIComponent(v) + '&api_url=' + encodeURIComponent(apiUrl) + '&port=' + port;
        var s = sectionSelect ? sectionSelect.value : '';
        if (s) existsUrl += '&section_id=' + encodeURIComponent(s);
        fetch(existsUrl, {
          headers: { 'Authorization': 'Token ' + token }
        })
          .then(function(r) { return r.ok ? r.json() : null; })
          .then(function(data) {
            if (!data) return;
            if (versionId !== v) return;
            datasetExists = !!data.exists;
            updateSyncButtonsState();
          })
          .catch(function() {});
      }
      function loadVssProjects(token) {
        if (!vssProjectSelect || !syncServiceUrl || !apiUrl || !token) return;
        vssProjectSelect.innerHTML = '<option value="">Loading…</option>';
        fetch(syncServiceUrl + '/vss-projects?project_id=' + project + '&api_url=' + encodeURIComponent(apiUrl), {
          headers: { 'Authorization': 'Token ' + token }
        })
          .then(function(r) {
            if (!r.ok) return r.json().then(function(d) { throw new Error(d.detail || r.statusText); });
            return r.json();
          })
          .then(function(vssProjects) {
            if (!vssProjects || vssProjects.length === 0) {
              if (vssProjectRow) vssProjectRow.style.display = 'none';
              vssProjectSelect.innerHTML = '<option value="">No VSS projects</option>';
              vssProjectsData = [];
              return;
            }
            vssProjectsData = vssProjects;
            if (vssProjectRow) vssProjectRow.style.display = '';
            vssProjectSelect.innerHTML = '';
            vssProjects.forEach(function(vp) {
              var opt = document.createElement('option');
              opt.value = vp.key;
              opt.textContent = vp.name + ' (' + vp.key + ')';
              vssProjectSelect.appendChild(opt);
            });
            if (vssProjectSelect.options.length) {
              vssProjectSelect.selectedIndex = 0;
              setVssProjectFromDropdown();
            }
          })
          .catch(function(err) {
            if (vssProjectRow) vssProjectRow.style.display = 'none';
            vssProjectSelect.innerHTML = '<option value="">Failed to load VSS projects</option>';
          });
      }
      function loadSections(token) {
        if (!sectionSelect || !syncServiceUrl || !apiUrl || !token) return;
        sectionSelect.innerHTML = '<option value="">Loading…</option>';
        fetch(syncServiceUrl + '/sections?project_id=' + project + '&api_url=' + encodeURIComponent(apiUrl), {
          headers: { 'Authorization': 'Token ' + token }
        })
          .then(function(r) {
            if (!r.ok) return r.json().then(function(d) { throw new Error(d.detail || r.statusText); });
            return r.json();
          })
          .then(function(sections) {
            sectionSelect.innerHTML = '';
            var allOpt = document.createElement('option');
            allOpt.value = '';
            allOpt.textContent = 'All sections';
            sectionSelect.appendChild(allOpt);
            (sections || []).forEach(function(s) {
              var opt = document.createElement('option');
              opt.value = String(s.id);
              opt.textContent = s.name + (s.id != null ? ' (' + s.id + ')' : '');
              sectionSelect.appendChild(opt);
            });
            if (initialSectionId && sectionSelect.querySelector('option[value="' + initialSectionId + '"]')) {
              sectionSelect.value = initialSectionId;
            }
          })
          .catch(function() {
            sectionSelect.innerHTML = '';
            var opt = document.createElement('option');
            opt.value = '';
            opt.textContent = 'All sections';
            sectionSelect.appendChild(opt);
            if (initialSectionId) sectionSelect.value = initialSectionId;
          });
      }
      function loadBoxTypes(token) {
        if (!boxTypeSelect || !syncServiceUrl || !apiUrl || !token) return;
        boxTypeSelect.innerHTML = '<option value="">Loading…</option>';
        fetch(syncServiceUrl + '/localization-types?project_id=' + project + '&api_url=' + encodeURIComponent(apiUrl), {
          headers: { 'Authorization': 'Token ' + token }
        })
          .then(function(r) {
            if (!r.ok) return r.json().then(function(d) { throw new Error(d.detail || r.statusText); });
            return r.json();
          })
          .then(function(boxTypes) {
            boxTypeSelect.innerHTML = '';
            var allOpt = document.createElement('option');
            allOpt.value = '';
            allOpt.textContent = 'All box types';
            boxTypeSelect.appendChild(allOpt);
            (boxTypes || []).forEach(function(t) {
              var opt = document.createElement('option');
              opt.value = String(t.id);
              opt.textContent = t.name + (t.id != null ? ' (' + t.id + ')' : '');
              boxTypeSelect.appendChild(opt);
            });
          })
          .catch(function() {
            boxTypeSelect.innerHTML = '';
            var opt = document.createElement('option');
            opt.value = '';
            opt.textContent = 'All box types';
            boxTypeSelect.appendChild(opt);
          });
      }
      function loadVersions(token) {
        if (!versionSelect || !syncServiceUrl || !apiUrl || !token) return;
        versionSelect.innerHTML = '<option value="">Loading…</option>';
        fetch(syncServiceUrl + '/versions?project_id=' + project + '&api_url=' + encodeURIComponent(apiUrl), {
          headers: { 'Authorization': 'Token ' + token }
        })
          .then(function(r) {
            if (!r.ok) return r.json().then(function(d) { throw new Error(d.detail || r.statusText); });
            return r.json();
          })
          .then(function(versions) {
            versionSelect.innerHTML = '';
            (versions || []).forEach(function(v) {
              var opt = document.createElement('option');
              opt.value = String(v.id);
              opt.textContent = v.name + (v.id != null ? ' (' + v.id + ')' : '');
              if (v.description) opt.title = v.description;
              opt.setAttribute('data-description', v.description || '');
              versionSelect.appendChild(opt);
            });if (initialVersionId && versionSelect.querySelector('option[value="' + initialVersionId + '"]')) {
              versionSelect.value = initialVersionId;
            } else if (versionSelect.options.length) {
              versionSelect.selectedIndex = 0;
            }// Set initial tooltip
            var selOpt = versionSelect.options[versionSelect.selectedIndex];
            versionSelect.title = selOpt ? (selOpt.getAttribute('data-description') || '') : '';
            setVersionFromDropdown();
          })
          .catch(function(err) {
            versionSelect.innerHTML = '';
            var opt = document.createElement('option');
            opt.value = '';
            opt.textContent = 'Failed to load versions';
            versionSelect.appendChild(opt);
            setVersionFromDropdown();
          });
      }
      if (tokenInput) {
        tokenInput.addEventListener('input', function() {
          setSyncControlsEnabled(false);
          if (versionSelect) {
            versionSelect.innerHTML = '';
            var opt = document.createElement('option');
            opt.value = '';
            opt.textContent = 'Enter token and click Test';
            versionSelect.appendChild(opt);
          }
          if (sectionSelect) {
            sectionSelect.innerHTML = '';
            var secOpt = document.createElement('option');
            secOpt.value = '';
            secOpt.textContent = 'Enter token and click Test';
            sectionSelect.appendChild(secOpt);
          }
          if (boxTypeSelect) {
            boxTypeSelect.innerHTML = '';
            var btOpt = document.createElement('option');
            btOpt.value = '';
            btOpt.textContent = 'Enter token and click Test';
            boxTypeSelect.appendChild(btOpt);
          }
          if (vssProjectSelect && vssProjectRow) {
            vssProjectSelect.innerHTML = '<option value="">Enter token and click Test</option>';
            vssProjectRow.style.display = 'none';
          }
          resetSyncDatasetChoices('Enter token and click Test');
          if (syncStatus) { syncStatus.textContent = ''; syncStatus.classList.remove('error'); }
        });
      }
      if (voxelDatasetSelect) voxelDatasetSelect.addEventListener('change', setSyncDatasetFromDropdown);
      if (vssProjectSelect) vssProjectSelect.addEventListener('change', setVssProjectFromDropdown);
      if (versionSelect) versionSelect.addEventListener('change', setVersionFromDropdown);
      if (sectionSelect) sectionSelect.addEventListener('change', function() {
        if (tokenVerified && hasDatabaseEntry && versionId) checkDatasetExists();
      });
      function updateDatabaseInfo(token) {
        if (!syncServiceUrl || !token) return;
        var databaseInfoUrl = syncServiceUrl + '/database-info?project_id=' + project + '&api_url=' + encodeURIComponent(apiUrl) + '&port=' + port;
        if (vssProjectKey) databaseInfoUrl += '&vss_project_key=' + encodeURIComponent(vssProjectKey);
        fetch(databaseInfoUrl, { headers: { 'Authorization': 'Token ' + token } })
          .then(function(r) {
            if (!r.ok) return r.json().catch(function() { return null; }).then(function(d) {
              var detail = (d && d.detail) ? d.detail : 'No database entry for this project/port';
              throw new Error(detail);
            });
            return r.json();
          })
          .then(function(d) {
            hasDatabaseEntry = true;
            isEnterprise = !!(d && d.is_enterprise);
            if (d && d.port != null) {
              port = d.port;
              appUrl = 'http://' + iframeHost + ':' + port + '/';
              console.log('[FiftyOne applet] database-info resolved: port=', port, 'database_name=', d.database_name, 'appUrl=', appUrl);
            }
            if (d && d.database_name) databaseName = d.database_name;
            if (d && d.database_uri) databaseUri = d.database_uri;
            var s3Row = document.getElementById('s3-bucket-row');
            if (s3Row) s3Row.style.display = isEnterprise ? '' : 'none';
            var s3BucketInput = document.getElementById('s3-bucket-input');
            var s3PrefixInput = document.getElementById('s3-prefix-input');
            if (s3BucketInput && d && d.s3_bucket) s3BucketInput.value = d.s3_bucket;
            if (s3PrefixInput && d && d.s3_prefix) s3PrefixInput.value = d.s3_prefix;
            setSyncControlsEnabled(true);
            syncStatus.textContent = 'Token OK. Sync enabled.';
            syncStatus.classList.remove('error');
            setTimeout(function() { syncStatus.textContent = ''; }, 3000);
            refreshSyncDatasetChoices();
          })
          .catch(function(err) {
            hasDatabaseEntry = false;
            var s3Row = document.getElementById('s3-bucket-row');
            if (s3Row) s3Row.style.display = 'none';
            setSyncControlsEnabled(true);
            syncStatus.textContent = 'Token OK but sync disabled: ' + (err.message || 'no database entry for this project');
            syncStatus.classList.add('error');
            refreshSyncDatasetChoices();
          });
      }
      if (testTokenBtn && syncStatus && syncServiceUrl && apiUrl) {
        testTokenBtn.addEventListener('click', function() {
          var token = getToken();
          if (!token) {
            syncStatus.textContent = 'Enter your API token first.';
            syncStatus.classList.add('error');
            return;
          }
          testTokenBtn.disabled = true;
          syncStatus.textContent = 'Testing token…';
          syncStatus.classList.remove('error');
          fetch(syncServiceUrl + '/versions?project_id=' + project + '&api_url=' + encodeURIComponent(apiUrl), {
            headers: { 'Authorization': 'Token ' + token }
          })
            .then(function(r) {
              if (!r.ok) return r.json().then(function(d) { throw new Error(d.detail || r.statusText); });
              return r.json();
            })
            .then(function(versions) {
              setSyncControlsEnabled(true);
              loadVersions(token);
              loadSections(token);
              loadBoxTypes(token);
              loadVssProjects(token);
              syncStatus.textContent = 'Token OK. Resolving port/database…';
              updateDatabaseInfo(token);
            })
            .catch(function(err) {
              setSyncControlsEnabled(false);
              syncStatus.textContent = 'Token invalid: ' + (err.message || 'Unknown error');
              syncStatus.classList.add('error');
              if (versionSelect) {
                versionSelect.innerHTML = '';
                var opt = document.createElement('option');
                opt.value = '';
                opt.textContent = 'Enter token and click Test';
                versionSelect.appendChild(opt);
              }
            })
            .finally(function() { testTokenBtn.disabled = false; });
        });
      }
      if (versionSelect) versionSelect.addEventListener('change', setVersionFromDropdown);
      function checkEmbeddingService() {
        var el = document.getElementById('embedding-status');
        if (!el) return;
        var base = syncServiceUrl || window.location.origin;
        var url = base.replace(/\/$/, '') + '/vss-embedding';
        var checkName = getSelectedVssProjectName();
        embeddingServiceReady = false;
        updateSyncButtonsState();
        fetch(url)
          .then(function(r) {
            if (!r.ok) {
              return r.json().catch(function() { return { detail: r.statusText }; }).then(function(d) {
                var detail = (d && d.detail) ? d.detail : '';
                if (detail.indexOf('not set') !== -1) {
                  embeddingServiceReady = true;
                  el.textContent = 'Not configured (Load from Tator works without embeddings)';
                  el.classList.remove('error');
                } else {
                  el.textContent = detail || 'Service unavailable';
                  el.classList.add('error');
                }
                updateSyncButtonsState();
                throw null;
              });
            }
            return r.json();
          })
          .then(function(data) {
            var projects = (data && data.projects) ? data.projects : [];
            var embeddingProject = getEmbeddingProject();
            if (!embeddingProject) {
              el.textContent = 'Select a VSS project (Verify Token first)';
              el.classList.add('error');
              updateSyncButtonsState();
              return;
            }
            if (checkName) {
              var inList = projects.indexOf(checkName) !== -1;
              el.textContent = inList
                ? 'Checking WebSocket…'
                : 'Available, but project not registered; checking WebSocket…';
              if (!inList) el.classList.add('error');
              else el.classList.remove('error');
            } else {
              el.textContent = 'Checking WebSocket…';
              el.classList.remove('error');
            }
            var wsTestUrl = base.replace(/\/$/, '') + '/vss-embedding/ws-test?project=' + encodeURIComponent(embeddingProject);
            return fetch(wsTestUrl).then(function(wsR) {
              if (wsR.ok) {
                embeddingServiceReady = true;
                if (checkName) {
                  var inList = projects.indexOf(checkName) !== -1;
                  el.textContent = inList
                    ? 'Available, project registered'
                    : 'Available, but project not registered; cannot compute embeddings or UMAP but you can still edit the localizations';
                  if (!inList) el.classList.add('error');
                  else el.classList.remove('error');
                } else {
                  el.textContent = 'Available (' + (projects.length || 0) + ' project(s)); set project_name tparam (vss_project) to check this project';
                  el.classList.remove('error');
                }
              } else {
                return wsR.json().catch(function() { return { detail: 'WebSocket test failed' }; }).then(function(d) {
                  el.textContent = (d && d.detail) ? d.detail : 'WebSocket test failed';
                  el.classList.add('error');
                });
              }
            });
          })
          .then(function() { updateSyncButtonsState(); })
          .catch(function(err) { if (err) { el.textContent = err.message || 'Network error'; el.classList.add('error'); updateSyncButtonsState(); } });
      }
      checkEmbeddingService();
      (function fetchAppVersion() {
        var base = syncServiceUrl || window.location.origin;
        fetch(base.replace(/\/$/, '') + '/version')
          .then(function(r) { return r.ok ? r.json() : null; })
          .then(function(d) {
            var el = document.getElementById('app-version');
            if (el && d && d.version) el.textContent = 'v' + d.version;
          })
          .catch(function() {});
      })();
      if (syncBtn && syncStatus && syncServiceUrl && apiUrl) {
        syncBtn.addEventListener('click', function() {
          var token = getToken();
          if (!token || !tokenVerified) return;
          var v = versionSelect ? versionSelect.value : '';
          syncBtn.disabled = true;
          syncStatus.textContent = 'Syncing…';
          syncStatus.classList.remove('error');
          if (fiftyoneAppLink) fiftyoneAppLink.style.display = 'none';
          var params = new URLSearchParams({ project_id: String(project), api_url: apiUrl, token: token, launch_app: 'true', port: port });
          if (v) params.set('version_id', v);
          var s = sectionSelect ? sectionSelect.value : '';
          if (s) params.set('section_id', s);
          var bt = boxTypeSelect ? boxTypeSelect.value : '';
          if (bt) params.set('localization_type_id', bt);
          if (syncQuery) params.set('query', syncQuery);
          if (vssProjectKey) params.set('vss_project_key', vssProjectKey);
          var forceSyncEl = document.getElementById('force-sync-checkbox');
          if (forceSyncEl && forceSyncEl.checked) {
            params.set('force_sync', 'true');
            params.set('force_embeddings', 'true');
            params.set('force_umap', 'true');
          }
          var verifiedOnlyEl = document.getElementById('verified-only-checkbox');
          if (verifiedOnlyEl && verifiedOnlyEl.checked) {
            params.set('verified_only', 'true');
          }
          var removeNearDupEl = document.getElementById('remove-near-duplicates-checkbox');
          if (removeNearDupEl && removeNearDupEl.checked) {
            params.set('remove_near_duplicates', 'true');
          }
          if (isEnterprise) {
            var s3BucketEl = document.getElementById('s3-bucket-input');
            var s3PrefixEl = document.getElementById('s3-prefix-input');
            if (s3BucketEl && s3BucketEl.value.trim()) params.set('s3_bucket', s3BucketEl.value.trim());
            if (s3PrefixEl && s3PrefixEl.value.trim()) params.set('s3_prefix', s3PrefixEl.value.trim());
          }
          var fullSyncUrl = syncServiceUrl + '/sync?' + params.toString();
          fetch(fullSyncUrl, { method: 'POST' })
            .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
            .then(function(result) {
              if (!result.ok) {
                syncStatus.textContent = 'Sync failed: ' + (result.data.detail || result.data.message || 'Unknown error');
                syncStatus.classList.add('error');
                syncBtn.disabled = false;
                return;
              }
              var data = result.data;
              if (data.job_id) {
                syncStatus.textContent = 'Sync queued. Waiting for worker…';
                var syncLogPanel = document.getElementById('sync-log-panel');
                if (syncLogPanel) { syncLogPanel.classList.remove('visible'); syncLogPanel.textContent = ''; }
                var statusUrl = syncServiceUrl + '/sync/status/' + encodeURIComponent(data.job_id);
                var logsUrl = syncServiceUrl + '/sync/logs/' + encodeURIComponent(data.job_id);
                function updateLogPanel(cb) {
                  fetch(logsUrl).then(function(r) { return r.ok ? r.json() : null; }).then(function(logData) {
                    if (syncLogPanel && logData && logData.log_lines && logData.log_lines.length) {
                      syncLogPanel.textContent = logData.log_lines.join('\n');
                      syncLogPanel.scrollTop = syncLogPanel.scrollHeight;
                    }
                    if (cb) cb();
                  }).catch(function() { if (cb) cb(); });
                }
                var logPanelHideTimeout = null;
                function hideLogPanelAfterDelay() {
                  if (logPanelHideTimeout) clearTimeout(logPanelHideTimeout);
                  logPanelHideTimeout = setTimeout(function() {
                    if (syncLogPanel) syncLogPanel.classList.remove('visible');
                    logPanelHideTimeout = null;
                  }, 30000);
                }
                var poll = function() {
                  fetch(statusUrl)
                    .then(function(r) { return r.json(); })
                    .then(function(s) {
                      if (s.status === 'queued' || s.status === 'started' || s.status === 'deferred') {
                        syncStatus.textContent = s.status === 'started' ? 'Sync in progress…' : 'Sync queued…';
                        if (syncLogPanel) syncLogPanel.classList.add('visible');
                        updateLogPanel(function() { setTimeout(poll, 2500); });
                        return;
                      }
                      if (s.status === 'failed') {
                        syncStatus.textContent = 'Sync failed: ' + (s.error || 'Unknown error');
                        syncStatus.classList.add('error');
                        syncBtn.disabled = false;
                        if (syncLogPanel) syncLogPanel.classList.add('visible');
                        updateLogPanel(hideLogPanelAfterDelay);
                        return;
                      }
                      if (s.status === 'finished' && s.result) {
                        var res = s.result;
                        if (!res || typeof res !== 'object' || Array.isArray(res)) {
                          res = {};
                        }
                        if (res.status === 'busy' || res.status === 'error') {
                          syncStatus.textContent = res.message || 'Sync failed. Please try again in a few minutes.';
                          syncStatus.classList.add('error');
                          syncBtn.disabled = false;
                          if (syncLogPanel) syncLogPanel.classList.add('visible');
                          updateLogPanel(hideLogPanelAfterDelay);
                          return;
                        }
                        syncStatus.textContent = res.sample_count != null
                          ? 'Sync done. ' + res.sample_count + ' samples.'
                          : 'Sync done.';
                        var baseUrl = (res.app_url && res.app_url.replace(/\/$/, '')) || ('http://' + iframeHost + ':' + (res.port != null ? res.port : port));
                        var openUrl = baseUrl + '/';
                        if (fiftyoneAppLink) {
                          fiftyoneAppLink.href = openUrl;
                          fiftyoneAppLink.style.display = '';
                        }
                        setTimeout(function() { syncStatus.textContent = ''; }, 5000);
                        syncBtn.disabled = false;
                        if (syncLogPanel) syncLogPanel.classList.add('visible');
                        updateLogPanel(hideLogPanelAfterDelay);
                        checkDatasetExists();
                        refreshSyncDatasetChoices();
                        return;
                      }
                      if (s.status === 'unknown') {
                        syncStatus.textContent = 'Sync status unknown: ' + (s.error || 'Job not found');
                        syncStatus.classList.add('error');
                        syncBtn.disabled = false;
                        if (syncLogPanel) syncLogPanel.classList.remove('visible');
                        return;
                      }
                      syncStatus.textContent = 'Sync: ' + (s.status || 'unknown');
                      if (syncLogPanel) syncLogPanel.classList.add('visible');
                      updateLogPanel(function() { setTimeout(poll, 2500); });
                    })
                    .catch(function(err) {
                      syncStatus.textContent = 'Status check failed: ' + (err.message || 'Network error');
                      syncStatus.classList.add('error');
                      syncBtn.disabled = false;
                      if (syncLogPanel) syncLogPanel.classList.remove('visible');
                    });
                };
                poll();
                return;
              }
              var dataObj = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
              syncStatus.textContent = dataObj.sample_count != null
                ? 'Sync done. ' + dataObj.sample_count + ' samples.'
                : 'Sync done.';
              var baseUrl = (dataObj.app_url || 'http://localhost:' + port).replace(/\/$/, '');
              var openUrl = baseUrl + '/';
              if (fiftyoneAppLink) {
                fiftyoneAppLink.href = openUrl;
                fiftyoneAppLink.style.display = '';
              }
              setTimeout(function() { syncStatus.textContent = ''; }, 5000);
              syncBtn.disabled = false;
              checkDatasetExists();
              refreshSyncDatasetChoices();
            })
            .catch(function(err) {
              syncStatus.textContent = 'Sync error: ' + (err.message || 'Network error');
              syncStatus.classList.add('error');
              syncBtn.disabled = false;
              refreshSyncDatasetChoices();
            });
        });
      }
      if (syncToTatorBtn && syncStatus && syncServiceUrl && apiUrl) {
        syncToTatorBtn.addEventListener('click', function() {
          var token = getToken();
          if (!token || !tokenVerified) return;
          var v = versionSelect && versionSelect.value ? versionSelect.value : '';
          if (!v) return;
          var dsName = selectedSyncDatasetName;
          if (!dsName) {
            syncStatus.textContent = 'Select a dataset mapping first.';
            syncStatus.classList.add('error');
            return;
          }
          syncToTatorBtn.disabled = true;
          syncStatus.textContent = 'Pushing to Tator…';
          syncStatus.classList.remove('error');
          var params = new URLSearchParams({
            project_id: String(project),
            version_id: v,
            api_url: apiUrl,
            token: token,
            port: String(port),
            database_name: databaseName,
            dataset_name: dsName
          });
          var fullUrl = syncServiceUrl + '/sync-to-tator?' + params.toString();
          fetch(fullUrl, { method: 'POST' })
            .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
            .then(function(result) {
              if (result.ok) {
                syncStatus.textContent = 'Sync to Tator done.';
                if (result.data.updated != null)
                  syncStatus.textContent += ' Updated: ' + result.data.updated;
                if (result.data.skipped != null)
                  syncStatus.textContent += ', Skipped: ' + result.data.skipped;
                if (result.data.failed != null && result.data.failed > 0)
                  syncStatus.textContent += ', Failed: ' + result.data.failed;
                setTimeout(function() { syncStatus.textContent = ''; }, 5000);
              } else {
                syncStatus.textContent = 'Sync to Tator failed: ' + (result.data.detail || result.data.message || 'Unknown error');
                syncStatus.classList.add('error');
              }
            })
            .catch(function(err) {
              syncStatus.textContent = 'Sync to Tator error: ' + (err.message || 'Network error');
              syncStatus.classList.add('error');
            })
            .finally(function() { setVersionFromDropdown(); });
        });
      }
      if (deleteDatasetBtn && syncStatus && syncServiceUrl && apiUrl) {
        deleteDatasetBtn.addEventListener('click', function() {
          var token = getToken();
          if (!token || !tokenVerified) return;
          var v = versionSelect ? versionSelect.value : '';
          if (!v) {
            syncStatus.textContent = 'Select a version first.';
            syncStatus.classList.add('error');
            return;
          }
          var versionLabel = versionSelect.options[versionSelect.selectedIndex]
            ? versionSelect.options[versionSelect.selectedIndex].textContent
            : 'version ' + v;
          if (!confirm('Delete FiftyOne dataset for ' + versionLabel + '? This cannot be undone.')) return;
          deleteDatasetBtn.disabled = true;
          syncStatus.textContent = 'Deleting dataset…';
          syncStatus.classList.remove('error');
          var params = new URLSearchParams({
            project_id: String(project),
            version_id: v,
            api_url: apiUrl,
            port: String(port)
          });
          var sec = sectionSelect ? sectionSelect.value : '';
          if (sec) params.set('section_id', sec);
          fetch(syncServiceUrl + '/delete-dataset?' + params.toString(), {
            method: 'POST',
            headers: { 'Authorization': 'Token ' + token }
          })
            .then(function(r) { return r.json().then(function(d) { return { ok: r.ok, data: d }; }); })
            .then(function(result) {
              if (result.ok) {
                var deleted = result.data.deleted;
                syncStatus.textContent = deleted
                  ? 'Deleted dataset: ' + deleted
                  : (result.data.message || 'No dataset found for this version.');
                syncStatus.classList.remove('error');
                if (fiftyoneAppLink) fiftyoneAppLink.style.display = 'none';
                setTimeout(function() { syncStatus.textContent = ''; }, 5000);
              } else {
                syncStatus.textContent = 'Delete failed: ' + (result.data.detail || result.data.message || 'Unknown error');
                syncStatus.classList.add('error');
              }
            })
            .catch(function(err) {
              syncStatus.textContent = 'Delete error: ' + (err.message || 'Network error');
              syncStatus.classList.add('error');
            })
            .finally(function() {
              datasetExists = false;
              deleteDatasetBtn.disabled = true;
              checkDatasetExists();
              refreshSyncDatasetChoices();
            });
        });
      }
    })();
  </script>
</body>
</html>
"""
