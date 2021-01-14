colName='clientes';
pkName='cli_id';
use mapfre;
db[colName].drop();
for(var i=0;i<15607;i++){
	doc={};
	doc[pkName]=i;
	db[colName].insert(doc);
}
docIndex={};
docIndex[pkName]=1;
db[colName].createIndex(docIndex);
