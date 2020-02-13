// The parser will replace {variableName} with variables provided by the RETL environment
// All functions (GetUrl, GenerateUUID, etc) take a single argument
{
  define: {
    nameTable: MongoCollection({url: "mongoURL", collection: "name"}),
    addressTable: MongoCollection({url: "mongoURL", collection: "address"})
  },

  /* Comments can go anywhere, and follow JavaScript conventions */
  start: [
    // DefaultStream() || (GetUrl({url: "https://www.somedata.zzz/datafile.zip"}) |> StreamZipContents()),
    TextToLines(),
    DelimitedRecordToArray({delimiter: ",", canBeEnclosedBy: "\""}),
    GenerateUUID({head: true}), // Inserts as the first element in the array, shift everything else to the right
    SendFieldsToBranch({targets: [
      {dest: "nameMongo", fields: [0..4]},
      {dest: "addressMongo", fields: [0,5..9]} // Array sent to address will contain 6 elements
    ]})
  ],
  addressMongo: [
    addressTable.save({indexMap: {
      recordID: 0,
      street: 1,
      city: 2,
      state: 3,
      zip: 4,
      "zip+4": 5
    }})
  ]
}
