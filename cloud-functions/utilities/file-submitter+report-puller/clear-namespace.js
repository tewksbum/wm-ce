const { Datastore } = require("@google-cloud/datastore");
const datastore = new Datastore();
const namespace = "dev-aam-rlh";
const kinds = ["people-fiber", "people-golden", "people-set", "record"];
(async () => {
  for (let index = 0; index < kinds.length; index++) {
    const k = kinds[index];
    const query = datastore.createQuery(namespace, k).limit(500);
    const [tasks] = await datastore.runQuery(query);
    var keys = tasks.map(function(res) {
      return res[datastore.KEY];
    });
    console.log(keys);
    await datastore.delete(keys);
  }
})();
