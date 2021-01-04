import path from 'path';
import fs from 'fs';
const fsPromises = fs.promises;

const maxChunkSize = 1024 * 1024 * 25; // 25MB

// in real life I would use something like yargs to handle command line arguments

// a valid command is of the form:
// node chunker.js (split | merge) inputPath1 outputPath1 [inputPath2 outputPath2 [inputPath3 outputPath3...]]
const args = process.argv.slice(2);
const commandWord = args[0];
const inputOutputPairs = splitArrayToSmallArrays(args.slice(1), 2);

let commandFunction;
switch (commandWord) {
    case 'split':
        commandFunction = splitFileToChunks;
        break;
    case 'merge':
        commandFunction = mergeChunksToFile;
        break;
    default:
        console.log('Command must be "split" or "merge"');
}

Promise.all(inputOutputPairs.map((inputOutput) => commandFunction(...inputOutput)))
    .catch(console.error);


async function splitFileToChunks(inputFile, outputDir) {
    try {
        await fsPromises.access(inputFile);
    } catch {
        throw new Error("Invalid input path");
    }
    let mkdirPromise = fsPromises.mkdir(outputDir, { recursive: true }); // in case directory doesnt exist
    let fileSize = (await fsPromises.stat(inputFile)).size;
    let numberOfFiles = Math.ceil(fileSize / maxChunkSize);
    let inputFileName = path.parse(inputFile).name;
    await mkdirPromise;

    return new Promise((resolve, reject) => {
        let readerStream = fs.createReadStream(inputFile, { highWaterMark: maxChunkSize });
        readerStream.on('error', reject);
        let metadata = {files: new Array(numberOfFiles)};
        let writePromises = new Array(numberOfFiles);
        let index = 0;
        readerStream.on('data', (chunk) => {
            let outputFile = path.normalize(`${outputDir}/${inputFileName}_${index}`);
            let i = index;
            let writeFilePromise = fsPromises.writeFile(outputFile, chunk)
                .then(() => metadata.files[i] = outputFile)
                .catch(() => { return reject(new Error("Cannot save chunk to file")); });
            writePromises[index] = writeFilePromise;
            index++;
        });
        readerStream.on('end', () => {
            let metadataFile = path.normalize(`${outputDir}/metadata.json`);
            Promise.all(writePromises)
                .then(() => fsPromises.writeFile(metadataFile, JSON.stringify(metadata)))
                .then(resolve)
                .catch(reject);
        });

    });
}

async function mergeChunksToFile(inputDir, outputFile) {
    try {
        await fsPromises.access(inputDir);
    } catch {
        throw new Error("Invalid input path");
    }
    let metadataString = await fsPromises.readFile(path.normalize(`${inputDir}/metadata.json`));
    let metadata = JSON.parse(metadataString);
    let files = metadata.files;
    // make sure all files exit
    let existenceArray = files.map((file) => fsPromises.access(file));
    await Promise.all(existenceArray).catch(() => { throw new Error("Cannot find all chunks") });

    await fsPromises.mkdir(path.dirname(outputFile), { recursive: true }); // in case directory doesnt exist
    let writerStream = fs.createWriteStream(outputFile);
    writerStream.on('error', (err) => {throw err});

    for (let i = 0; i < files.length; i++){
        await readAndPipePromise(files[i], writerStream);
    }

    writerStream.end();
}

/**
 * Take a file, create a read stream and pipe it to a given open write stream.
 * Returns a promise that will resolve when the reading ends.
 * 
 * @param {String} fileToRead A file to read from by creating a read stream.
 * @param {fs.WriteStream} [writeStream] A write stream into which we pipe. Must be already open and will remain open afterwards.
 * @returns {Promise} A promise that will be resolved when the reading ends.
 */
function readAndPipePromise(fileToRead, writeStream) {
    return new Promise((resolve, reject) => {
        let rs = fs.createReadStream(fileToRead);
        rs.on('error', reject);
        rs.pipe(writeStream, { end: false });
        rs.on('end', resolve);
    });
}

function splitArrayToSmallArrays(array, smallArraySize){
    var smallArrays = [];
    while (array.length > 0){
        smallArrays.push(array.splice(0, smallArraySize));
    }
    return smallArrays;
}