import fs from 'fs';

const MAX_CHUNK_SIZE = 1024 * 1024 * 25; // 25MB

const inputPath = "./input/sampleFile";
const outputPath = "./output";
let readerStream = fs.createReadStream(inputPath, {highWaterMark: MAX_CHUNK_SIZE});

// we can compute ahead how many files are needed and create them all now
const baseChunkName = "Chunk";
let index = 0;
readerStream.on('data', function(chunk) {
    console.log(chunk.length);
    index++;
    let outputFile = `${outputPath}/${baseChunkName}_${index}`;
    fs.writeFile(outputFile, chunk, function (err) {
        if (err){
            throw err;
        }
    });
});

readerStream.on('end', function(){
    console.log('done');
})