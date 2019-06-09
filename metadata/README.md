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


## Old Info

### General metadata engine

 * Questions
   * What are the triggers?
     * Inception of a new (potential) target object
     * Manual push of metadata that others might depend on
     * An autonomous process that watches metadata of some kind and pushes results of calculations
   * How to access data stored in repository?
   * How to prevent that several metadata extraction processes step on each toes?
     * Create the same metadata twice
     * When writing out to the repo
 * Possible process:
   * injection creates InjectionData manually
   * there's a process that watches injection data and keeps state of all missing metadata (but how big can that data become?)
     * it tries to execute automatic extractors in topological order, dependencies first
     * how does this automatic process know about the latest versions of everything?
     * when the process is stopped it could either forget everything and start reprocessing from scratch or write a snapshot
       with the remaining work
       * reprocessing from scratch needs:
         * enumerating all objects
         * access all metadata per object
 * Metadata Repository:
   * Gives access to all stored metadata
   * Writes metadata to different places safely i.e. without several threads (processes?) appending to the same file