import fs from 'fs';

const maxChunkSize = 1024 * 1024 * 25; // 25MB

const inputPath = "./input/sampleFile";
const outputPath = "./output";
const mergeOutput = "./output/mergedFile"

// splitFileToChunks(inputPath, outputPath);

mergeChunksToFile(outputPath, mergeOutput);



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
    let metadataString = fs.readFileSync(`${inputDir}/metadata.json`); // TODO: make async
    let metadata = JSON.parse(metadataString);
    // TODO: if some files are missing, throw an error and return
    
    let writerStream = fs.createWriteStream(outputFile);
    for (let file of metadata.files) {
        let data = fs.readFileSync(file); // TODO: make async
        writerStream.write(data);
    }
    writerStream.end();
}
