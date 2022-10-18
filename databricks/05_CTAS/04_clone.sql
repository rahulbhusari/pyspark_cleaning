-- two options to efficiently copy Delta Lake tables
-- data modifications applied to the cloned version of the table will be tracked and stored separately from the source.



-- DEEP CLONE fully copies the data and metadata from a source table to a target.
CREATE OR REPLACE TABLE purchases_deep_clone
DEEP CLONE purhcases ;

/*
source_table_size   source_num_of_files num_removed_files   num_copied_files    removed_files_size  copied_files_size
142117              4                   0                   4                   0                   142117
*/


-- SHALLOW CLONE only copies the Delta transaction logs, meaning that the data does not move.
CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purhcases;

/*
source_table_size   source_num_of_files num_removed_files   num_copied_files    removed_files_size  copied_files_size
142117              4                   0                   0                   0                   0
*/