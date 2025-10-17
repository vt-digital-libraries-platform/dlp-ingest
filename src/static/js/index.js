document.addEventListener("DOMContentLoaded", function() {
    // Populate table dropdown from AWS
    fetch('/api/tables')
        .then(response => response.json())
        .then(data => {
            const select = document.getElementById("dynamodb_table_select");
            select.innerHTML = "";
            data.tables.forEach(table => {
                const option = document.createElement("option");
                option.value = table;
                option.textContent = table;
                select.appendChild(option);
            });
        });

    // When a table is selected, autofill the suffix field
    document.getElementById("dynamodb_table_select").addEventListener("change", function() {
        const tableName = this.value;
        let suffix = "";
        const hyphenIndex = tableName.indexOf("-");
        if (hyphenIndex !== -1 && tableName.length > hyphenIndex + 1) {
            suffix = tableName.substring(hyphenIndex + 1);
        }
        document.getElementById("dynamodb_table_suffix").value = suffix;
        document.getElementById("dynamodb_table_suffix").dispatchEvent(new Event("change"));
    });

    function fetchIdentifiers() {
        const suffix = document.getElementById("dynamodb_table_suffix").value;
        fetch(`/api/identifiers?suffix=${encodeURIComponent(suffix)}`)
            .then(response => response.json())
            .then(data => {
                const datalist = document.getElementById("collection_identifiers");
                datalist.innerHTML = "";
                data.identifiers.forEach(id => {
                    const option = document.createElement("option");
                    option.value = id;
                    datalist.appendChild(option);
                });
            });
    }

    // Initial fetch
    fetchIdentifiers();

    // Prepopulate fields for detected environment
    document.getElementById("dynamodb_table_suffix").addEventListener("change", function() {
        const suffix = this.value;
        const envDev = document.getElementById("env_dev");
        const envPreprod = document.getElementById("env_preprod");
        const envProd = document.getElementById("env_prod");
        const envMessage = document.getElementById("env_message");

        // Reset checkboxes
        envDev.checked = false;
        envPreprod.checked = false;
        envProd.checked = false;
        envMessage.textContent = "";

        // Remove all hardcoded value assignments here
        // (No document.getElementById(...).value = "..." lines)

        // Environment detection logic remains, but field population will be handled by setEnvFields(env)
        if (suffix.endsWith("vtdlpdev")) {
            envDev.checked = true;
            envMessage.textContent = "Detected environment: Dev";
        } else if (suffix.endsWith("vtdlppprd")) {
            envPreprod.checked = true;
            envMessage.textContent = "Detected environment: Preprod";
        } else if (suffix.endsWith("vtdlpprd")) {
            envProd.checked = true;
            envMessage.textContent = "Detected environment: Prod";
        }

        fetchIdentifiers();
        checkAllSections();
    });

    document.getElementById("ingest_button").addEventListener("click", function(e) {
        const identifier = document.getElementById("collection_identifier").value;
        if (!identifier.trim()) {
            e.preventDefault();
            alert("Please fill in the Collection Identifier before submitting.");
            document.getElementById("collection_identifier").focus();
        }
    });

    document.getElementById("ingest_button").addEventListener("click", function(e) {
        e.preventDefault(); // Prevent normal form submission

        const form = document.querySelector("form");
        const formData = new FormData(form);
        const xhr = new XMLHttpRequest();
        const progressBar = document.getElementById("progress-bar");
        const progressText = document.getElementById("progress-text");

        xhr.upload.onprogress = function(event) {
            if (event.lengthComputable) {
                const percent = Math.round((event.loaded / event.total) * 100);
                progressBar.value = percent;
                progressText.textContent = percent + "%";
            }
        };

        xhr.onload = function() {
            progressBar.value = 100;
            progressText.textContent = "Complete!";
            // Redirect to a results or success page
            window.location.href = "/success"; // Change "/success" to your desired URL
        };

        xhr.open("POST", form.action);
        xhr.send(formData);

        progressBar.value = 0;
        progressText.textContent = "Uploading...";
    });

    // --- Section checker logic ---
    const sections = [
        {
            id: "aws-section",
            statusId: "aws-section-status",
            fields: ["aws_src_bucket", "aws_dest_bucket"]
        },
        {
            id: "collection-section",
            statusId: "collection-section-status",
            // Only check collection_category for completion
            fields: ["collection_category"]
        },
        {
            id: "dynamodb-section",
            statusId: "dynamodb-section-status",
            fields: ["dynamodb_noid_table", "dynamodb_file_char_table"]
        },
        {
            id: "path-section",
            statusId: "path-section-status",
            fields: ["app_img_root_path", "long_url_path", "short_url_path"]
        },
        {
            id: "noid-section",
            statusId: "noid-section-status",
            fields: ["noid_scheme", "noid_naa"]
        },
        {
            id: "media-section",
            statusId: "media-section-status",
            fields: ["media_type"]
        }
    ];

    function checkSection(section) {
        let filled = 0;
        section.fields.forEach(id => {
            const el = document.getElementById(id);
            if (el && el.value && el.value.trim() !== "") {
                filled++;
                // Highlight completed fields
                el.classList.add("field-complete");
            } else if (el) {
                el.classList.remove("field-complete");
            }
        });
        const status = document.getElementById(section.statusId);
        if (!status) return;
        if (filled === section.fields.length) {
            status.textContent = "Complete";
            status.classList.remove("incomplete");
            status.classList.add("complete");
            // Do NOT hide the section
            document.getElementById(section.id).classList.remove("closed");
        } else {
            status.textContent = `Incomplete (${filled}/${section.fields.length})`;
            status.classList.remove("complete");
            status.classList.add("incomplete");
            document.getElementById(section.id).classList.remove("closed");
        }
    }

    function checkAllSections() {
        sections.forEach(checkSection);
    }

    // Section checker: listen for changes
    document.querySelectorAll("input, select").forEach(el => {
        el.addEventListener("input", checkAllSections);
    });

    // Initial check
    checkAllSections();

    // Add summary panel logic
    const summaryBtn = document.getElementById("goto-missing");
    if (summaryBtn) {
        summaryBtn.onclick = function() {
            for (const section of sections) {
                const status = document.getElementById(section.statusId);
                if (status && status.classList.contains("incomplete")) {
                    document.getElementById(section.id).scrollIntoView({behavior: "smooth"});
                    break;
                }
            }
        };
    }

    const select = document.getElementById("collection_identifier");
    const status = document.getElementById("collection-identifier-status");

    function checkCollectionIdentifier() {
        // For a select menu, check if a value is selected
        if (select.value && select.value.trim() !== "") {
            status.textContent = "Complete";
            status.classList.remove("incomplete");
            status.classList.add("complete");
        } else {
            status.textContent = "Incomplete";
            status.classList.remove("complete");
            status.classList.add("incomplete");
        }
    }

    select.addEventListener("change", checkCollectionIdentifier);
    select.addEventListener("input", checkCollectionIdentifier); // For manual typing if supported
    window.addEventListener("DOMContentLoaded", checkCollectionIdentifier);

    let envDefaults = {};

    fetch('/api/env_defaults')
        .then(response => response.json())
        .then(data => {
            envDefaults = data;
            // Optionally, set initial environment fields here if you want a default
        });

    function setEnvFields(env) {
        const defaults = envDefaults[env];
        if (!defaults) return;
        document.getElementById("aws_src_bucket").value = defaults.aws_src_bucket || "";
        document.getElementById("aws_dest_bucket").value = defaults.aws_dest_bucket || "";
        document.getElementById("collection_category").value = defaults.collection_category || "";
        document.getElementById("dynamodb_noid_table").value = defaults.dynamodb_noid_table || "";
        document.getElementById("dynamodb_file_char_table").value = defaults.dynamodb_file_char_table || "";
        document.getElementById("app_img_root_path").value = defaults.app_img_root_path || "";
        document.getElementById("long_url_path").value = defaults.long_url_path || "";
        document.getElementById("short_url_path").value = defaults.short_url_path || "";
        document.getElementById("noid_scheme").value = defaults.noid_scheme || "";
        document.getElementById("noid_naa").value = defaults.noid_naa || "";
    }

    // When the suffix changes, detect environment and set fields
    document.getElementById("dynamodb_table_suffix").addEventListener("change", function() {
        const suffix = this.value;
        const envDev = document.getElementById("env_dev");
        const envPreprod = document.getElementById("env_preprod");
        const envProd = document.getElementById("env_prod");
        const envMessage = document.getElementById("env_message");

        // Reset checkboxes
        envDev.checked = false;
        envPreprod.checked = false;
        envProd.checked = false;
        envMessage.textContent = "";

        if (suffix.endsWith("vtdlpdev")) {
            envDev.checked = true;
            envMessage.textContent = "Detected environment: Dev";
            setEnvFields("dev");
        } else if (suffix.endsWith("vtdlppprd")) {
            envPreprod.checked = true;
            envMessage.textContent = "Detected environment: Preprod";
            setEnvFields("preprod");
        } else if (suffix.endsWith("vtdlpprd")) {
            envProd.checked = true;
            envMessage.textContent = "Detected environment: Prod";
            setEnvFields("prod");
        }

        fetchIdentifiers();
        checkAllSections();
    });

    document.getElementById("ingest_button").addEventListener("click", function(e) {
        const form = document.querySelector("form");
        const checkboxes = form.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(cb => {
            // Remove any existing hidden field for this checkbox
            const existing = form.querySelector(`input[type="hidden"][name="${cb.name}"]`);
            if (existing) existing.remove();

            // If checked, value is true; if not checked, add a hidden field with value false
            if (!cb.checked) {
                const hidden = document.createElement("input");
                hidden.type = "hidden";
                hidden.name = cb.name;
                hidden.value = "false";
                form.appendChild(hidden);
            } else {
                cb.value = "true";
            }
        });
        // Allow form to submit normally after this
    });
});