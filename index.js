import fs from 'fs';
const fsPromises = fs.promises;

const maxChunkSize = 1024 * 1024 * 25; // 25MB

const inputPath = "./input/sampleFile";
const outputPath = "./output";
const mergeOutput = "./output/mergedFile"

// splitFileToChunks(inputPath, outputPath);

mergeChunksToFile(outputPath, mergeOutput).catch((err) => console.error(err));



const baseChunkName = "Chunk";
function splitFileToChunks(inputFile, outputDir) {
    let readerStream = fs.createReadStream(inputFile, {highWaterMark: maxChunkSize});
    let metadata = {files: [], numberOfFiles: undefined}; // numberOfFiles can already be calculated, and array can be initalized with size
    let index = 0;
    readerStream.on('data', function(chunk) {
        console.log(chunk.length);
        let outputFile = `${outputDir}/${baseChunkName}_${index}`;
        fs.writeFile(outputFile, chunk, function (err) {
            if (err){
                throw err;
            }
        });
        metadata.files.push(outputFile); // see above
        index++;
    });

    readerStream.on('end', function(){
        // TODO: wait till all writings succeed
        metadata.numberOfFiles = index;
        fs.writeFile(`${outputDir}/metadata.json`, JSON.stringify(metadata),function (err) {
            if (err){
                throw err;
            }
        });
        // resolve promise after writing the metadata
        console.log('done');
    })

}

function mergeChunksToFile(inputDir, outputFile) {

    return new Promise((resolve, reject) => {

        let filesPromise = fsPromises.readFile(`${inputDir}/metadata.json`)
            .then(JSON.parse)
            .then((metadata) => metadata.files);
        
        // break the chain. would have been more elegant with async/await
        let checkFilesPromise = filesPromise.then((files) => {
                return files.map((file) => fsPromises.access(file));
            })
            .then((accessArray) => Promise.all(accessArray))
            .catch(() => reject(new Error("Cannot find all chunks")));
    
    let writerStream = fs.createWriteStream(outputFile);

        Promise.all([filesPromise, checkFilesPromise])
            .then(([files,_]) => {
                // TODO: handle errors
                let p = Promise.resolve();
                for (let i = 0; i < files.length; i++){
                    p = p.then(() => fsPromises.readFile(files[i]))
                        .then((data) => writerStream.write(data))
                        // .catch((err) => reject(err));
    }
                return p;
            })
            .then(() => writerStream.end())
            .then(resolve, reject);

    });

}
