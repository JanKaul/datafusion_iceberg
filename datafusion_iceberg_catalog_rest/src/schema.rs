use std::{any::Any, sync::Arc};

use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result},
};
use iceberg_rs::{
    catalog::{namespace::Namespace, table_identifier::TableIdentifier, Catalog},
    datafusion::DataFusionTable,
    model::schema::{SchemaStruct, SchemaV2},
};

pub struct DataFusionSchema {
    schema: Namespace,
    catalog: Arc<dyn Catalog>,
}

impl DataFusionSchema {
    pub fn new(schema: Namespace, catalog: Arc<dyn Catalog>) -> Self {
        DataFusionSchema { schema, catalog }
    }
}

impl SchemaProvider for DataFusionSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        let tables = futures::executor::block_on(self.catalog.list_tables(&self.schema));
        match tables {
            Err(_) => vec![],
            Ok(schemas) => schemas.into_iter().map(|x| x.name().to_owned()).collect(),
        }
    }
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let mut full_name = Vec::from(self.schema.levels().clone());
        full_name.push(name.to_owned());
        let table = futures::executor::block_on(
            Arc::clone(&self.catalog).load_table(TableIdentifier::try_new(&full_name).ok()?),
        )
        .ok()?;
        Some(Arc::new(DataFusionTable::from(table)))
    }
    fn table_exist(&self, name: &str) -> bool {
        let mut full_name = Vec::from(self.schema.levels().clone());
        full_name.push(name.to_owned());
        let identifier = TableIdentifier::try_new(&full_name);
        match identifier {
            Err(_) => false,
            Ok(identifier) => futures::executor::block_on(self.catalog.table_exists(&identifier))
                .ok()
                .unwrap_or(false),
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut full_name = Vec::from(self.schema.levels().clone());
        full_name.push(name.to_owned());
        let table = futures::executor::block_on(
            Arc::clone(&self.catalog).create_table(
                TableIdentifier::try_new(&full_name)
                    .map_err(|err| DataFusionError::Internal(format!("{}", err)))?,
                SchemaV2 {
                    struct_fields: SchemaStruct::try_from(table.schema().as_ref())
                        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?,
                    schema_id: 0,
                    identifier_field_ids: None,
                    name_mapping: None,
                },
            ),
        )
        .map_err(|err| DataFusionError::Internal(format!("{}", err)))?;
        Ok(Some(Arc::new(DataFusionTable::from(table))))
    }
    fn deregister_table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(None)
    }
}
