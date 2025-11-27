const getTables = async () => {
    // Populate table dropdown from AWS
    const response = await fetch('/api/tables')
        .then(response => response.json())
        .then(data => {
            const select = document.getElementById("dynamodb_table_suffix");
            select.innerHTML = "";
            data.tables.forEach(table => {
                const env = table.split('-').pop(); // Get the last part of the table name
                const value = table.replace("Collection-", ""); // Remove environment suffix for value
                const option = document.createElement("option");
                option.value = value;
                option.textContent = env;
                select.appendChild(option);
            });
        });
}

// Fetch environment defaults from the server
const getDefaults = async () => {
    const response = await fetch('/api/env_defaults')
    const defaults = await response.json();
    return defaults;
}

const handleRadioChange = async (event) => {  
    const defaults = await getDefaults();
    if (event.target.value === "other") {
        document.getElementById("db-table-select").classList.remove("hidden");
    }
    else {
        await setEnvFields(defaults,event.target.value);
        await fetchIdentifiers();
        checkAllSections();
    }
}


const resetMediaTypes = () => {
    document.getElementById("3d-options").classList.add("hidden");
    document.getElementById("3d-options").querySelectorAll("input").forEach(input => {
        input.value = "";
    });
}


const _3dSelected = () => {
    document.getElementById("3d-options").classList.remove("hidden");
}


const handleMediaTypeChange = (event) => {
    const mediaType = document.getElementById("media_type").value;
    switch (mediaType) {
        case "3d":
            _3dSelected();
            break;
        case "3d_2diif":
            _3dSelected();
            break;
        default:
            // Reset to default state
            resetMediaTypes();
            break;
    }
}

const addListeners = async () => {
    // Add event listener for media type selection
    document.getElementById("media_type").addEventListener("change", (event) => {
        handleMediaTypeChange();
    });
    
    // Add event listeners for environment selection (radio buttons)
    for (let elem of document.querySelectorAll('input[type="radio"][name="ENV_SELECTION"]')) {
        elem.addEventListener("change", handleRadioChange);
    };


    // When a table is selected, autofill the suffix field
    document.getElementById("dynamodb_table_suffix").addEventListener("change", function(event) {
        const envDev = document.getElementById("env_dev");
        const envPprd = document.getElementById("env_pprd");

        // Remove all hardcoded value assignments here
        // (No document.getElementById(...).value = "..." lines)

        // Environment detection logic remains, but field population will be handled by setEnvFields(env)
        if (event.target.value.endsWith("vtdlpdev")) {
            envDev.checked = true;
        } else if (event.target.value.endsWith("vtdlppprd")) {
            envPprd.checked = true;
        }

        fetchIdentifiers();
        checkAllSections();
    });


    document.getElementById("ingest_button").addEventListener("click", function(e) {
        e.preventDefault(); // Prevent normal form submission

        const identifier = document.getElementById("collection_identifier").value;
        if (!identifier.trim()) {
            e.preventDefault();
            alert("Please fill in the Collection Identifier before submitting.");
            document.getElementById("collection_identifier").focus();
        }

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


    // Section checker: listen for changes on errthang
    document.querySelectorAll("input, select").forEach(el => {
        el.addEventListener("input", checkAllSections);
    });


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
    select.addEventListener("change", checkCollectionIdentifier);
    select.addEventListener("input", checkCollectionIdentifier); // For manual typing if supported
    window.addEventListener("DOMContentLoaded", checkCollectionIdentifier);


    document.getElementById("ingest_button").addEventListener("click", function(e) {
        const form = document.querySelector("form");
        const checkboxes = form.querySelectorAll('input[type="checkbox"]');
        checkboxes.forEach(cb => {
            // Remove any existing hidden field for this checkbox
            const existing = form.querySelector(`input[type="hidden"][name="${cb.name}"]`);
            existing && existing.remove();

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
}


const fetchIdentifiers = async () => {
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


const checkSection = (section) => {
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


const checkAllSections = () => {
    for (const section of sections) {
        checkSection(section);
    }
}


const checkCollectionIdentifier = () => {
    const select = document.getElementById("collection_identifier");
    const status = document.getElementById("collection-identifier-status");
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


const setEnvFields = (defaults, env) => {
    const envDefaults = defaults[env];
    if (!envDefaults) return;
    document.getElementById("aws_src_bucket").value = envDefaults.aws_src_bucket || "";
    document.getElementById("aws_dest_bucket").value = envDefaults.aws_dest_bucket || "";
    document.getElementById("collection_category").value = envDefaults.collection_category || "";
    document.getElementById("dynamodb_table_suffix").value = envDefaults.dynamodb_table_suffix || "";
    document.getElementById("dynamodb_noid_table").value = envDefaults.dynamodb_noid_table || "";
    document.getElementById("dynamodb_file_char_table").value = envDefaults.dynamodb_file_char_table || "";
    document.getElementById("app_img_root_path").value = envDefaults.app_img_root_path || "";
    document.getElementById("long_url_path").value = envDefaults.long_url_path || "";
    document.getElementById("short_url_path").value = envDefaults.short_url_path || "";
    document.getElementById("noid_scheme").value = envDefaults.noid_scheme || "";
    document.getElementById("noid_naa").value = envDefaults.noid_naa || "";
}

const init = async () => {
    // Fetch dynamoDB tables from aws
    await getTables();

    // Fetch environment defaults and set fields
    await getDefaults().then((def) => {
        setEnvFields(def,"pprd");
        checkAllSections();
    })

    // Fetch identifiers for the collection identifier field, based on table
    fetchIdentifiers();

    // Add event listeners
    addListeners();
}


document.addEventListener("DOMContentLoaded", function() {
    init();
});