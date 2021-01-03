import fs from 'fs';
const fsPromises = fs.promises;

const maxChunkSize = 1024 * 1024 * 25; // 25MB

const inputPath = "./input/sampleFile";
const outputPath = "./output";
const mergeOutput = "./output/mergedFile"

splitFileToChunks(inputPath, outputPath).catch((err) => console.error(err));

// mergeChunksToFile(outputPath, mergeOutput).catch((err) => console.error(err));

// TODO: deal with relative and absolute pathes


const baseChunkName = "Chunk";
function splitFileToChunks(inputFile, outputDir) {

    return new Promise((resolve, reject) => {
        
    let readerStream = fs.createReadStream(inputFile, {highWaterMark: maxChunkSize});
        readerStream.on('error', (err) => reject(err));
    let metadata = {files: [], numberOfFiles: undefined}; // numberOfFiles can already be calculated, and array can be initalized with size
        let writeFilePromises = []; // see above
    let index = 0;
        readerStream.on('data', (chunk) => {
        console.log(chunk.length);
        let outputFile = `${outputDir}/${baseChunkName}_${index}`;
            let i = index;
            let writeFilePromise = fsPromises.writeFile(outputFile, chunk)
                .then(() => metadata.files[i] = outputFile) // see above; dont write full path 
                .catch(() => {throw new Error('Cannot save chunk to file')});
            writeFilePromises[index] = writeFilePromise;
        index++;
        });
        readerStream.on('end', () => {
            metadata.numberOfFiles = index; // see above
            Promise.all(writeFilePromises)
                .then(() => fsPromises.writeFile(`${outputDir}/metadata.json`, JSON.stringify(metadata)))
                .then(resolve, reject);
    });

        });
}

function mergeChunksToFile(inputDir, outputFile) {

    return new Promise((resolve, reject) => {

        let filesPromise = fsPromises.readFile(`${inputDir}/metadata.json`)
            .then(JSON.parse)
            .then((metadata) => metadata.files);
        
        // break the chain. would have been more elegant with async/await.
        // checking if files exists in parallel isn't really helpful but whatever
        let checkFilesPromise = filesPromise.then((files) => {
                return files.map((file) => fsPromises.access(file));
            })
            .then((accessArray) => Promise.all(accessArray))
            .catch(() => { throw new Error("Cannot find all chunks") });
    
    let writerStream = fs.createWriteStream(outputFile);
        writerStream.on('error', (err) => reject(err));

        Promise.all([filesPromise, checkFilesPromise])
            .then(([files,_]) => {
                // TODO: handle errors
                let p = Promise.resolve();
                for (let i = 0; i < files.length; i++){
                    p = p.then(() => readAndPipePromise(files[i], writerStream));
    }
                return p;
            })
            .then(() => writerStream.end())
            .then(resolve, reject);

    });

}

/**
 * Take a file, create a read stream and pipe it to a given open write stream.
 * Returns a promise that will resolve when the reading ended.
 * 
 * @param {String} fileToRead A file to read from by creating a read stream.
 * @param {fs.WriteStream} [writeStream] A write stream into which we pipe. Must be already open and will remain open afterwards.
 * @returns {Promise} A promise that will be resolved when the reading ended.
 */
function readAndPipePromise(fileToRead, writeStream) {
    return new Promise((resolve, reject) => {
        let rs = fs.createReadStream(fileToRead);
        rs.on('error', (err) => reject(err));
        rs.pipe(writeStream, { end: false });
        rs.on('end', () => resolve());
    });
}

