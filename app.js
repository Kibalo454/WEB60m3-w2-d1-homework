const { MongoClient } = require('mongodb');

// Set TASK = 1..8 before each run
const TASK = 8;

const uri = 'mongodb://127.0.0.1:27017';
const client = new MongoClient(uri);

async function main() {
  await client.connect();
  const db = client.db('statsdb');
  const col = db.collection('uscensus');

  const seed = [
    { City: 'Corona',   Zip: '11368', State: 'NY', Income: 0,     Age: 0 },
    { City: 'Los Angeles', Zip: '90001', State: 'CA', Income: 42000, Age: 35 },
    { City: 'San Diego',   Zip: '92101', State: 'CA', Income: 51000, Age: 34 },
    { City: 'Anchorage',   Zip: '99501', State: 'AK', Income: 37000, Age: 44 }
  ];

  switch (TASK) {
    case 1:
      console.log('Task 1: Database "statsdb" created/selected.');
      break;

    case 2:
      if ((await db.listCollections({ name: 'uscensus' }).toArray()).length === 0) {
        await db.createCollection('uscensus');
        console.log('Task 2: Collection "uscensus" created.');
      } else console.log('Task 2: Collection already exists.');
      break;

    case 3:
      if ((await col.countDocuments()) === 0) {
        const r = await col.insertMany(seed);
        console.log(`Task 3: Inserted starter docs: ${r.insertedCount}`);
      } else console.log('Task 3: Starter data present.');
      break;

    case 4:
      const extra = [
        { City: 'Pacoima', Zip: '91331', State: 'CA', Income: 60360, Age: 33 },
        { City: 'Ketchikan', Zip: '99950', State: 'AK', Income: 0, Age: 0 }
      ];
      const r4 = await col.insertMany(extra);
      console.log(`Task 4: Inserted ${r4.insertedCount} new docs.`);
      break;

    case 5:
      const doc5 = await col.findOne({ City: 'Corona', State: 'NY' }, { projection: { Zip: 1, _id: 0 } });
      console.log(doc5 ? `Task 5: Corona, NY ZIP = ${doc5.Zip}` : 'Task 5: Corona, NY not found.');
      break;

    case 6:
      const res6 = await col.find({ State: 'CA' }, { projection: { City: 1, Income: 1, _id: 0 } }).toArray();
      console.log('Task 6: Income for CA cities:');
      res6.forEach(r => console.log(`- ${r.City}: ${r.Income}`));
      break;

    case 7:
      const r7 = await col.updateMany({ State: 'AK' }, { $set: { Income: 38910, Age: 46 } });
      console.log(`Task 7: Updated AK docs: ${r7.modifiedCount}`);
      break;

    case 8:
      const docs8 = await col.find({}, { projection: { City: 1, State: 1, Zip: 1, Income: 1, Age: 1, _id: 0 } })
        .sort({ State: 1 })
        .toArray();
      console.log('Task 8: Sorted by State (ascending):');
      docs8.forEach(d => console.log(`${d.State} | ${d.City} | ${d.Zip} | Income ${d.Income} | Age ${d.Age}`));
      break;

    default:
      console.log('Set TASK = 1..8');
  }

  await client.close();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
