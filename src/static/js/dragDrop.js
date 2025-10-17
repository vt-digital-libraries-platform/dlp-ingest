const dropContainer = document.getElementById('dropContainer');

dropContainer.ondragover = dropContainer.ondragenter = function(evt) {
    evt.preventDefault();
};

dropContainer.ondrop = function(evt) {
    evt.stopPropagation();
    evt.preventDefault();
    fileInput.files = evt.dataTransfer.files;

    // const dT = new DataTransfer();
    // dT.items.add(evt.dataTransfer.files[0]);
    // dT.items.add(evt.dataTransfer.files[3]);
    // fileInput.files = dT.files;
    console.log(fileInput.files)
    
};
