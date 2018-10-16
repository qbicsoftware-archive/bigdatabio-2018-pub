#!/usr/bin/env groovy

/**
 * Queries against PRIDE database via its REST API.
 *
 * Questions:
 *
 *     1. How many projects per year since 2005?
 *     2. Data volume per year since 2005?
 *
 * @author: Sven Fillinger <sven.fillinger@qbic.uni-tuebingen.de>
 * @year: 2018
 *
 */
import groovy.json.JsonSlurper

// An output path, needs to be specified
def OUTPUT_PATH = '/home/myHome'
// Will contain the query result object
def queryRes = []
// Groovy JSON parser
def jSlurper = new JsonSlurper()

/**
 * A little hardcoded, but it turned out, that 20 pages
 * are sufficient to query all the projects information,
 * given a page size of 1000 entries.
 */
 for (page in 0..20) {
     def connection = new URL("https://www.ebi.ac.uk:443/pride/ws/archive/project/list?show=1000&page=$page").openConnection()
     if ( connection.content ) {
         queryRes << jSlurper.parse(connection.content)
     }
}

// A map that contains all project accessions and their registration date
Map projectDateMap = [:]
queryRes.each { it.'list'.each { projectDateMap."${it.accession}" = "${it.publicationDate}"  } }

// Write the project accession and date out to a tsv file
File output = new File([OUTPUT_PATH, "project_accessions_and_date.tsv "].join(File.separator))
output.withWriter { writer ->
    projectDateMap.sort { e1, e2 -> e1.value <=> e2.value }.each { 
        writer.write( "${it.key}\t${it.value}\n" )
    }
}

/**
 * Query for all files in a project and their filesizes in Bytes
 */
String project_api_endpoint = "https://www.ebi.ac.uk:443/pride/ws/archive/file/list/project/"
File prideAccessions = new File([OUTPUT_PATH, "project_accessions_and_date.tsv"].join( File.separator ))
Map dateAccessionMap = [:]
def content = prideAccessions.readLines()
// Gather all accessions per registration date
content.each {
    def (accession, date) = it.split("\t")
    if (! dateAccessionMap."$date"){
        dateAccessionMap."$date" = [accession]
    } else {
        dateAccessionMap."$date" += accession
    }
}

// Iterate over the dates and sum the files volume for this date
dateVolumeMap = [:]
/**
 * Small helper method that computes the total project data volume
 * for a given accession number.
 * @param accession The project's accession number
 * @param apiURL The api endpoint (URL)
 * @return The total volume of a project in Bytes
 */
Long getFilesVolumeForProject(String accession, String apiURL) {
    if (accession in ["PXD003180", "PXD006214", "PXD010293"]) // We observed API request errors for these accessions.
        return 0L
    JsonSlurper slurper = new JsonSlurper()
    // Small timer to not trigger the request limit
    sleep(50)

    Map json = slurper.parse(new URL("$apiURL$accession").openConnection().content)
    Long totalVolume = 0L
    json.'list'.each {
        totalVolume += it.'fileSize' as Long
    }
    return totalVolume
}

output = new File([OUTPUT_PATH, "file_volumes_per_date.tsv"].join(File.separator))

dateAccessionMap.each { date, accessionList ->
        accessionList.each { accession ->
            Long volume = getFilesVolumeForProject(accession, project_api_endpoint)
             if (!dateVolumeMap."$date") {
                 dateVolumeMap."$date" = volume as Long
             } else {
                 dateVolumeMap."$date" += volume as Long
             }
           }
        output << "$date\t${dateVolumeMap[date]}\n"
}




 