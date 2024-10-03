const {Client}=require('@elastic/elasticsearch')
const client=new Client({node:'http://localhost:9200'})

class EmployeeDatabase {
    async createCollection(p_collection_name) {
        const exists = await client.indices.exists({ index: p_collection_name });
        if (exists.body) {
            await client.indices.delete({ index: p_collection_name });
            console.log(`Existing collection (index) ${p_collection_name} deleted.`);
        }
        await client.indices.create({ index: p_collection_name });
        console.log(`New collection (index) ${p_collection_name} created.`);
    }
    async indexData(p_collection_name, p_exclude_column) {
        const sample_data=[{EmployeeID:"E02001",Name:"John Doe",Department:"IT",Gender:"Male"},
            {EmployeeID:"E02002",Name:"Jane Smith",Department:"HR",Gender:"Female"},
            {EmployeeID:"E02003",Name:"Sam Brown",Department:"IT",Gender:"Male"},
            {EmployeeID:"E02004",Name:"Lucy Black",Department:"Finance",Gender:"Female"}]
        for(const record of sample_data){
            const { [p_exclude_column]: excluded, ...indexed_record } = record;
            if (Object.keys(indexed_record).length === 0) {
                console.error('No valid data to index after exclusion.');
                continue;
            }
            if (!indexed_record || typeof indexed_record !== 'object') {
                console.error('Indexed record is not valid:', indexed_record);
                continue;
            }
            console.log('Indexing record:', indexed_record);
            try {
                await client.index({
                    index: p_collection_name,  
                    document: indexed_record 
                });
            }catch(error){
                // console.error('Error indexing document:', error);
            }
        }
        await client.indices.refresh({ index: p_collection_name });
        console.log(`Data indexed in ${p_collection_name}, excluding column: ${p_exclude_column}`);
    }
    
    

    async searchByColumn(p_collection_name, p_column_name, p_column_value) {
        try {
            const result = await client.search({
                index: p_collection_name,
                body: { 
                    query: {
                        match: {
                            [p_column_name]: p_column_value
                        }
                    }
                }
            });
       
            return result.body.hits.hits.map(hit=>hit._source);
        }catch(error){
            console.error('Error searching by column:', error);
        }
    }
    

    async getEmpCount(p_collection_name) {
        const result = await client.count({ index: p_collection_name });
        return result.body.count;
    }

    async delEmpById(p_collection_name, p_employee_id) {
        const result = await client.deleteByQuery({
            index: p_collection_name,
            body: {
                query: {
                    match: { EmployeeID: p_employee_id }
                }
            }
        });
        console.log(`Deleted employee with ID ${p_employee_id} from ${p_collection_name}`);
    }

    async getDepFacet(p_collection_name) {
        try {
            const result = await client.search({
                index: p_collection_name,
                body: { 
                    size: 0, 
                    aggs: {
                        department_count: {
                            terms: {
                                field: "Department"
                            }
                        }
                    }
                }
            });
            return result.body.aggregations.department_count.buckets;
        }catch(error){
            console.error('Error getting department facets:', error);
        }
    }
    
}

(async () => {
    const db = new EmployeeDatabase();

    const v_nameCollection = 'kirubakaran'; 
    const v_phoneCollection = '5935'; 

    await db.createCollection(v_nameCollection);
    await db.createCollection(v_phoneCollection);

    console.log("Initial Employee Count:", await db.getEmpCount(v_nameCollection));

    await db.indexData(v_nameCollection, 'Department');
    await db.indexData(v_phoneCollection, 'Gender');

    await db.delEmpById(v_nameCollection, 'E02003');

    console.log("Employee Count after deletion:", await db.getEmpCount(v_nameCollection));

    console.log("Search by Department (IT):", await db.searchByColumn(v_nameCollection, 'Department', 'IT'));
    console.log("Search by Gender (Male):", await db.searchByColumn(v_nameCollection, 'Gender', 'Male'));
    console.log("Search by Department (IT) in phone collection:", await db.searchByColumn(v_phoneCollection, 'Department', 'IT'));

    console.log("Department facet for name collection:", await db.getDepFacet(v_nameCollection));
    console.log("Department facet for phone collection:", await db.getDepFacet(v_phoneCollection));
})();
