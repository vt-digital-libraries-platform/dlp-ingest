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

const handleEnvRadioChange = async (event) => {  
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


const handleIngestTypeChange = async (event) => {  
    const subCollectionOptions = document.getElementById("sub_collection-options");
    const msgArchive = document.getElementById("msg-archive");
    const msgCollection = document.getElementById("msg-collection");
    if (event.target.value === "collection") {
        subCollectionOptions.classList.remove("hidden");
        // Show the collection message, hide the archive message
        msgArchive.classList.add("hidden");
        msgCollection.classList.remove("hidden");
        checkAllSections();
    }
    else {
        subCollectionOptions.classList.add("hidden");
        // Show the archive message, hide the collection message
        msgArchive.classList.remove("hidden");
        msgCollection.classList.add("hidden");
        checkAllSections();
    }
}


const resetMediaTypes = () => {
    hide3dOptions();
    document.getElementById("3d_options").querySelectorAll("input").forEach(input => {
        input.value = "";
    });
    document.getElementById("3d_options").querySelectorAll("select").forEach(input => {
        input.value = "";
    });
}


const show3dOptions = () => {
    const ingestType = document.getElementById("ingest_type-archive")
    if(ingestType.checked) {
        document.getElementById("3d_options").classList.remove("hidden");
    }
}


const hide3dOptions = () => {
    document.getElementById("3d_options").classList.add("hidden");
    hideFlashCardOptions();
}


const handleMediaTypeChange = (event) => {
    const mediaType = document.getElementById("media_type").value;
    switch (mediaType) {
        case "3d":
            show3dOptions();
            break;
        case "3d_2diiif":
            show3dOptions();
            break;
        default:
            // Reset to default state
            resetMediaTypes();
            break;
    }
}

const showFlashCardOptions = () => {
    document.getElementById("3d_options-flash_card-options").classList.remove("hidden");
}


const hideFlashCardOptions = () => {
    document.getElementById("3d_options-flash_card-options").classList.add("hidden");
}

const checkCollectionAndParentIdentifiers = (selected, other) => {
    const selectedElement = document.getElementById(selected);
    const selected_identifier = selectedElement.value;
    const other_identifier = document.getElementById(other).value;
    const match = document.getElementById("collection_parent_match");

        if(selected_identifier === other_identifier) {
            match.classList.remove("hidden");
            selectedElement.value = "";
            window.setTimeout(() => {
                match.classList.add("hidden");
            }, 5000);
        }
}

// Add event listeners for all the form elements
const addListeners = async () => {

    // parent collection identifier
    document.getElementById("parent_collection_identifier").addEventListener("change", function(event) {
        checkCollectionAndParentIdentifiers("parent_collection_identifier", "collection_identifier");
    });
    
    // File input
    document.getElementById("metadata_input").addEventListener("change", function(event) {
        const fileInput = event.target;
        if(fileInput.files?.length > 0){
            document.getElementById("metadata_input").classList.remove("incomplete");
        }
    });

    // Add event listeners for ingest type selection (radio buttons)
    for (let elem of document.querySelectorAll('input[type="radio"][name="INGEST_TYPE"]')) {
        elem.addEventListener("change", handleIngestTypeChange);
    };



    document.getElementById("3d_options-addOns").addEventListener("change", (event) => {
        const selectedValue = event.target.value;
        switch (selectedValue) {
            case "flash_card":
                showFlashCardOptions();
                break;
            default:
                hideFlashCardOptions();
                break;
        }
    });

    // Add event listener for media type selection
    document.getElementById("media_type").addEventListener("change", (event) => {
        handleMediaTypeChange();
    });
    
    // Add event listeners for environment selection (radio buttons)
    for (let elem of document.querySelectorAll('input[type="radio"][name="ENV_SELECTION"]')) {
        elem.addEventListener("change", handleEnvRadioChange);
    };


    // When a table is selected, autofill the suffix field
    document.getElementById("dynamodb_table_suffix").addEventListener("change", function(event) {
        const envDev = document.getElementById("env_dev");
        const envPprd = document.getElementById("env_pprd");

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
            const collection_datalist = document.getElementById("collection_identifiers");
            const parent_collection_datalist = document.getElementById("parent_collection_identifiers");
            // Clear existing options
            collection_datalist.innerHTML = "";
            parent_collection_datalist.innerHTML = "";
            collection_datalist.innerHTML = "";

            const selected_collection = document.getElementById("collection_identifier").value;
            data.identifiers.forEach(identifier => {
                const option = document.createElement("option");
                option.value = identifier;
                collection_datalist.appendChild(option);
                if (!selected_collection || identifier !== selected_collection) {
                    parent_collection_datalist.appendChild(option.cloneNode(true));
                }
            });
        });
}


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
    if (!status) return false;
    if (filled === section.fields.length) {
        status.textContent = "Complete";
        status.classList.remove("incomplete");
        status.classList.add("complete");
        // Do NOT hide the section
        document.getElementById(section.id).classList.remove("closed");

        return true;
    } else {
        status.textContent = `Incomplete (${filled}/${section.fields.length})`;
        status.classList.remove("complete");
        status.classList.add("incomplete");
        document.getElementById(section.id).classList.remove("closed");
        return false;
    }
}


const checkAllSections = () => {
    let allComplete = true;
    for (const section of sections) {
      if(!checkSection(section)) {
        allComplete = false;
      }
    }
    if (allComplete) {
        document.getElementById("ingest_button").classList.remove("disabled");
        document.getElementById("ingest_button").disabled = false;
    }
}


const checkCollectionIdentifier = () => {
    checkCollectionAndParentIdentifiers("collection_identifier", "parent_collection_identifier");
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

    if (!envDefaults) {
        alert("Default values not found for environment")
        document.getElementById("aws_src_bucket").value = "";
        document.getElementById("aws_dest_bucket").value = "";
        document.getElementById("collection_category").value = "";
        document.getElementById("dynamodb_table_suffix").value = "";
        document.getElementById("dynamodb_noid_table").value = "";
        document.getElementById("dynamodb_file_char_table").value = "";
        document.getElementById("app_img_root_path").value = "";
        document.getElementById("long_url_path").value = "";
        document.getElementById("short_url_path").value = "";
        document.getElementById("noid_scheme").value = "";
        document.getElementById("noid_naa").value = "";
    }
    else {
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
    },
    {
        id: "metadata-section",
        statusId: "metadata-section-status",
        fields: ["metadata_input"]
    }
];


async function init() {
    // Fetch dynamoDB tables from aws
    await getTables();

    // Fetch environment defaults and set fields
    await getDefaults().then((def) => {
        setEnvFields(def, "pprd");
        checkAllSections();
    });

    // Fetch identifiers for the collection identifier field, based on table
    fetchIdentifiers();

    // Add event listeners
    addListeners();
}


document.addEventListener("DOMContentLoaded", function() {
    init();
});