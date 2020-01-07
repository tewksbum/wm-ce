const { Datastore } = require("@google-cloud/datastore");

const datastore = new Datastore();

async function getOwners(env) {
  datastore.namespace = `wemade-${env}`;
  const query = datastore.createQuery("Customer");
  const [customers] = await datastore.runQuery(query);
  var owners = [];
  customers.forEach(function(customer, index) {
    var owner = customer.Owner;
    var school = owner.substr(0, 3).toUpperCase();
    if (!owners[school]) {
      owners[school] = {
        Owner: customer.Owner,
        AccessKey: customer.AccessKey
      };
    }
  });
  return owners;
}

module.exports.getOwners = getOwners;
