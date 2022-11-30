use std::{any::Any, sync::Arc};

use datafusion::{
    catalog::schema::SchemaProvider,
    datasource::TableProvider,
    error::{DataFusionError, Result},
};
use iceberg_rs::catalog::{identifier::Identifier, namespace::Namespace};

use crate::mirror::Mirror;

pub struct IcebergSchema {
    schema: Namespace,
    catalog: Arc<Mirror>,
}

impl IcebergSchema {
    pub(crate) fn new(schema: Namespace, catalog: Arc<Mirror>) -> Self {
        IcebergSchema { schema, catalog }
    }
}

impl SchemaProvider for IcebergSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        let tables = self.catalog.table_names(&self.schema);
        match tables {
            Err(_) => vec![],
            Ok(schemas) => schemas.into_iter().map(|x| x.name().to_owned()).collect(),
        }
    }
    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.catalog.table(
            Identifier::try_new(&[self.schema.levels(), &[name.to_string()]].concat()).unwrap(),
        )
    }
    fn table_exist(&self, name: &str) -> bool {
        self.catalog.table_exists(
            Identifier::try_new(&[self.schema.levels(), &[name.to_string()]].concat()).unwrap(),
        )
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut full_name = Vec::from(self.schema.levels().clone());
        full_name.push(name.to_owned());
        let identifier = Identifier::try_new(&full_name)
            .map_err(|err| DataFusionError::Internal(err.to_string()))?;
        self.catalog.register_table(identifier, table)
    }
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut full_name = Vec::from(self.schema.levels().clone());
        full_name.push(name.to_owned());
        let identifier = Identifier::try_new(&full_name)
            .map_err(|err| DataFusionError::Internal(err.to_string()))?;
        self.catalog.deregister_table(identifier)
    }
}
