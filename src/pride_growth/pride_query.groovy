/**
Queries against ENA and PRIDE database

Questions:

    1. How many projects per year since 2000?
    2. Data volume per year since 2000?
*/

import groovy.json.JsonSlurper

def OUTPUT_PATH = '/home/sven/Nextcloud/cloud_private/publications'

/**
def queryRes = []
def jSlurper = new JsonSlurper()

for (page in 0..20) {
     def connection = new URL("https://www.ebi.ac.uk:443/pride/ws/archive/project/list?show=1000&page=$page").openConnection()
     if ( connection.content ) {
         queryRes << jSlurper.parse(connection.content)
     }
}

Map projectDateMap = [:]

queryRes.each { it.'list'.each { projectDateMap."${it.accession}" = "${it.publicationDate}"  } }

File output = new File([OUTPUT_PATH, "accessions_years.tsv"].join(File.separator))
println output
projectDateMap
output.withWriter { writer ->
    projectDateMap.sort { e1, e2 -> e1.value <=> e2.value }.each { 
        writer.write( "${it.key}\t${it.value}\n" )
    }
}

*/

// Snippet for file per project query

def project_api_endpoint = "https://www.ebi.ac.uk:443/pride/ws/archive/file/list/project/"
File prideAccessions = new File([OUTPUT_PATH, "accessions_years.tsv"].join( File.separator ))
Map dateAccessionMap = [:]
def content = prideAccessions.readLines()
content.each {
    def (accession, date) = it.split("\t")
    if (! dateAccessionMap."$date"){
        dateAccessionMap."$date" = [accession]
    } else {
        dateAccessionMap."$date" += accession
    }
}

// Iterate over the dates and sum the files volum for this date
dateVolumeMap = [:]

Long getFilesVolumeForProject(String accession, String apiURL) {
    if (accession in ["PXD003180", "PXD006214", "PXD010293"])
        return 0
    JsonSlurper slurper = new JsonSlurper()
    sleep(50)
    Map json = slurper.parse(new URL("$apiURL$accession").openConnection().content)
    
    Long totalVolume = 0L
    json.'list'.each {
        println totalVolume as Long
        totalVolume += it.'fileSize' as Long
    }
    return totalVolume
}

File output = new File([OUTPUT_PATH, "date_volume.tsv"].join(File.separator))

dateAccessionMap.each { date, accessionList ->
    def dateformat = new Date().parse("yyyy-MM-dd", date)
    if (dateformat > new Date().parse("yyyy-MM-dd", "2018-09-04")) {
        accessionList.each {
            def volume = getFilesVolumeForProject(it, project_api_endpoint)
             if (!dateVolumeMap."$date") {
                 dateVolumeMap."$date" = volume as Long
             } else {
                 dateVolumeMap."$date" += volume as Long
             }
           }
        output << "$date\t${dateVolumeMap[date]}\n"
       }
   
    
}




 