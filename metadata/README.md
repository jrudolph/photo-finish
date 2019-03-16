# A general purpose metadata engine

Allows to store and efficeiently retrieve arbitrary (but strongly typed), versioned metadata for any kind of other
object.

Metadata can
 * be automatically extraced or calculated from the base object
 * be pushed by internal or external processes
 * form dependencies between different kinds of metadata that are automatically calculated in order and recreated
   when dependency data has changed
 * be versioned so that when a new version comes out the old metadata can be migrated to the new format
   or completely recalculated from the base object
