const { getOwners } = require("./utils");
(async () => {
  const owners = await getOwners("dev");
  console.log(owners["CSU"].Owner);
})();
