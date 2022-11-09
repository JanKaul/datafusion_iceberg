use std::{any::Any, sync::Arc};

use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    error::Result,
};
use iceberg_rs::catalog::{namespace::Namespace, Catalog};

use crate::schema::DataFusionSchema;

pub struct DatafusionCatalog {
    catalog: Arc<dyn Catalog>,
}

impl CatalogProvider for DatafusionCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema_names(&self) -> Vec<String> {
        let namespaces = futures::executor::block_on(self.catalog.list_namespaces(None));
        match namespaces {
            Err(_) => vec![],
            Ok(namespaces) => namespaces.into_iter().map(|x| x.to_string()).collect(),
        }
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let namespaces = self.schema_names();
        namespaces.iter().find(|x| *x == name).and_then(|y| {
            Some(Arc::new(DataFusionSchema::new(
                Namespace::try_new(&y.split(".").map(|z| z.to_owned()).collect::<Vec<String>>())
                    .ok()?,
                Arc::clone(&self.catalog),
            )) as Arc<dyn SchemaProvider>)
        })
    }

    fn register_schema(
        &self,
        _name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        unimplemented!()
    }
}
