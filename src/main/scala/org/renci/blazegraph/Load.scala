package org.renci.blazegraph

import java.io.{File, FileInputStream}

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection
import com.bigdata.rdf.store.DataLoader
import org.apache.commons.io.FileUtils
import org.apache.jena.sys.JenaSystem
import org.backuity.clist._
import org.openrdf.model._
import org.openrdf.rio.helpers.RDFHandlerBase
import org.openrdf.rio.{RDFFormat, Rio}
import java.util.Properties
import scala.jdk.CollectionConverters._

import me.tongfei.progressbar._
import java.io._

object Load extends Command(description = "Load triples") with Common with GraphSpecific {

  var base = opt[String](default = "")
  var useOntologyGraph = opt[Boolean](default = false, name = "use-ontology-graph", description = "Load triples into graph using the ontology IRI. Ignored in the case of a quads file format.")
  var dataFiles = args[Seq[File]]()

  def inputFormat: RDFFormat = informat.getOrElse("turtle").toLowerCase match {
    case "turtle"    => RDFFormat.TURTLE
    case "ttl"       => RDFFormat.TURTLE
    case "rdfxml"    => RDFFormat.RDFXML
    case "rdf-xml"   => RDFFormat.RDFXML
    case "ntriples"  => RDFFormat.NTRIPLES
    case "n-triples" => RDFFormat.NTRIPLES
    case "nt"        => RDFFormat.NTRIPLES
    case "n-quads"   => RDFFormat.NQUADS
    case "nquads"    => RDFFormat.NQUADS
    case "nq"        => RDFFormat.NQUADS
    case "trig"      => RDFFormat.TRIG
    case other       => throw new IllegalArgumentException(s"Invalid input RDF format: $other")
  }
  
 //def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
 //   val p = new java.io.PrintWriter(f)
 //   try { op(p) } finally { p.close() }
 // } 
  
  def runUsingConnection(blazegraph: BigdataSailRepositoryConnection): Unit = {
    JenaSystem.init()
    val tripleStore = blazegraph.getSailConnection.getTripleStore
    //val loaderOptions = scala.collection.mutable.HashMap.empty[String,String]
    //loaderOptions += ("DEFAULT_IGNORE_INVALID_FILES" -> "true")
    val loaderProperties = new Properties
    //loaderOptions.foreach { case (key, value) => loaderProperties.setProperty(key, value) }
    loaderProperties.setProperty("DEFAULT_IGNORE_INVALID_FILES", "true")
    val loader = new DataLoader(loaderProperties, tripleStore)
    val filesToLoad = dataFiles.flatMap(data => if (data.isFile) List(data) else FileUtils.listFiles(data, inputFormat.getFileExtensions.asScala.toArray, true).asScala).filter(_.isFile)
    //val invalidFiles: List[String] = List()
    val errorFiles = scala.collection.mutable.HashMap.empty[String,String]
    //var pb = new ProgressBar(filesToLoad.foreach.size)
    //pb.showSpeed = false
    var pb = new ProgressBar("Ingest progress", filesToLoad.size - 1) 
    
    filesToLoad.foreach { file =>
      try {
      scribe.info(s"Loading $file")
      println("")
      val ontGraphOpt = if (useOntologyGraph && !inputFormat.supportsContexts) findOntologyURI(file) else None
      val determinedGraphOpt = ontGraphOpt.orElse(graphOpt)
      val stats = loader.loadFiles(file, base, inputFormat, determinedGraphOpt.getOrElse(file.toURI.toString), null) 
      scribe.info(stats.toString)
       } catch {
         case x: java.lang.IllegalArgumentException => {
          //invalidFiles =  invalidFiles + s"$file"
          errorFiles += (s"$file" -> x.getMessage())
          println(x.getMessage())
         }
      } finally {
            pb.step()
         }
    }
    loader.endSource()
    //val file = File("fileErrors");
    //println("File error summary:")
    //println(errorFiles)
    //if(errorFiles.size == 0){
    //  println("No file errors!")  
    //} 
    if(errorFiles.size > 0) {
      //errorFiles.foreach(elem => println(elem[0] + " => " + elem[1]))
      println("File error summary:")
      errorFiles.foreach { case (key, value) => println(key + " => " + value) }
    }
    //printToFile(new File("fileErrors.txt")) { p =>
    //   errorFiles.foreach(p.println)
    // }
    tripleStore.commit()
  }

  /**
    * Tries to efficiently find the ontology IRI triple without loading the whole file.
    */
  def findOntologyURI(file: File): Option[String] = {
    object Handler extends RDFHandlerBase {
      override def handleStatement(statement: Statement): Unit = if (statement.getObject.stringValue == "http://www.w3.org/2002/07/owl#Ontology" &&
        statement.getPredicate.stringValue == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") throw FoundTripleException(statement)
    }
    val inputStream = new FileInputStream(file)
    try {
      val parser = Rio.createParser(inputFormat)
      parser.setRDFHandler(Handler)
      parser.parse(inputStream, base)
      // If an ontology IRI triple is found, it will be thrown out
      // in an exception. Otherwise, return None.
      None
    } catch {
      case FoundTripleException(statement) => {
        if (statement.getSubject.isInstanceOf[BNode]) {
          scribe.warn(s"Blank node subject for ontology triple: $statement")
          None
        } else Option(statement.getSubject.stringValue)
      }
    } finally {
      inputStream.close()
    }
  }

  final case class FoundTripleException(statement: Statement) extends RuntimeException

}
