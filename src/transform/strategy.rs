use crate::extract::contextualized_data_frame::ContextualizedDataFrame;

pub trait Strategy {
    fn transform<'a>(&self, table: &'a mut ContextualizedDataFrame) -> &'a ContextualizedDataFrame {
        if self.is_valid(table) {
            self.internal_transform(table)
        } else {
            table
        }
    }

    fn is_valid(&self, table: &ContextualizedDataFrame) -> bool;

    fn internal_transform<'a>(
        &self,
        table: &'a mut ContextualizedDataFrame,
    ) -> &'a ContextualizedDataFrame;
}
