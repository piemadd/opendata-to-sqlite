import fs from "node:fs";
import { Database } from "bun:sqlite";
import Papa from "papaparse";

//constants/config
const BASE_URL = "https://data.ny.gov/resource/jsu2-fbtj.json"; // export => api => json => copy link

//initial fetch for db setup
const initRes = await fetch(`${BASE_URL.replace('resource', 'api/views')}?$limit=0&$offset=0`);
const initData = await initRes.json();
const totalRows = Number(initData.columns[0].cachedContents.count);

let rowCount = 0;

// types for db setup
let columnTypes = {};
initData.columns.forEach((column) => {
  switch (column.dataTypeName) {
    case "number":
      columnTypes[column.fieldName] = 'REAL';
      break;
    case "text":
    case "calendar_date":
      columnTypes[column.fieldName] = "TEXT";
      break;
    default:
      columnTypes[column.fieldName] = "TEXT"; //fallback
  }
});

// functions to stringify each column properly for insertion into the db
let columnStringify = {};
initData.columns.forEach((column) => {
  switch (column.dataTypeName) {
    case "number":
      columnStringify[column.fieldName] = (n) => n; //lol
      break;
    case "text":
    case "calendar_date":
      columnStringify[column.fieldName] = (n) => '"' + n + '"'; // this is somehow faster than using a `` with lots of repitition
      break;
    default:
      columnStringify[column.fieldName] = (n) => '"' + JSON.stringify(n).replaceAll('"', '""') + '"'; //fallback
  }
});

// for insertion into db
const keyOrder = Object.keys(columnStringify);

//opening the db and creating the table if it doesn't exist
//takes the data types we fetched above and shoved them in
const db = new Database("data.sqlite", { create: true });
db.query(`
CREATE TABLE IF NOT EXISTS data (
  ${Object.keys(columnTypes).map((column) => `${column} ${columnTypes[column]}`).join(',\n')});`).run();

//enabling WAL, increasing write speed
db.exec("PRAGMA journal_mode = WAL;");

//functions for the data processing step
const processRow = (row, keys) => {
  let final = [];

  keys.forEach((key) => {
    final.push(columnStringify[key](row[key])); //stringify the data properly and add it to the row array
  })

  return `(${final.join(',')})`;
};

const processChunk = (chunk, keys) => chunk.map((row) => processRow(row, keys)).join(',\n');

const stream = fs.createReadStream('./out.csv', { encoding: 'utf8' });

let chunkBuffer = [];

Papa.parse(stream, {
  header: true,
  step: (results, parser) => {
    chunkBuffer.push(results.data);
    rowCount++;

    if (rowCount % 50000 == 0) {
      db.query(`
        INSERT INTO data (${keyOrder.join(',')})
        VALUES
          ${processChunk(chunkBuffer, keyOrder)};`).run();
      chunkBuffer = new Array();
      console.log(`Done with ${rowCount}/${totalRows} (${((rowCount / totalRows) * 100).toFixed(2)}%)`);
    }
  },
  complete: (results, file) => {
    db.query(`
      INSERT INTO data (${keyOrder.join(',')})
      VALUES
        ${processChunk(chunkBuffer, keyOrder)};`).run();
    console.log(`Done with ${rowCount}/${totalRows} (${((rowCount / totalRows) * 100).toFixed(2)}%)`);
    console.log("Parsing complete");
  },
  skipEmptyLines: true,
  dynamicTyping: false,
})
/*

// i could (and honestly should) use workers for the downloading, but I don't wanna get ratelimited, want to keep the code simple, and workers are experimental as of bun v1.1.29
let offset = 0;
let numLast = STEP; // a big number to start, helps judge when we're at the end

// possibility of infinite runaway, but eh
while (STEP <= numLast) {
  const res = await fetch(`${BASE_URL}?$limit=${STEP}&$offset=${offset}&$$app_token=${process.env.APP_TOKEN}`);
  const data = await res.json();

  if (data.length < 1) break;

  db.query(`
    INSERT INTO data (${keyOrder.join(',')})
    VALUES
      ${processChunk(data, keyOrder)};`).run();
  offset += data.length;

  console.log(`Done with ${offset}/${totalRows} rows`)
};
*/