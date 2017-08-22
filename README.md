# CloudEC
CloudEC is a MapReduce-based algorithm for correcting errors in next-generation
sequencing big data. It is released under Apache License 2.0 as a Free and Open 
Source Software Project.

## Requirement
  - Hadoop cluster of version 1.2.1 (recommend) or higher.
  - Java runtime environment (JRE) version 8.
  - Sequencing data in the FastQ format.

## Execution
1. Convert FastQ format data to the internal-used SimpleFastQ (SFQ) format using
other tools or the following command.
    > cat {FILENAME}.fastq
      | awk 'NR%4==1 {printf "%s\t", substr($0, 2)} NR%4==2 {printf "%s\t", $0}
      NR%4==0 {printf "%s\n", $0}' > {FILENAME}.sfq
2. Upload the SFQ format data to HDFS.
    > hadoop fs -put {FILENAME}.sfq {FILENAME}.sfq
3. Run CloudEC.
    > hadoop jar CloudEC.jar -in {FILENAME}.sfq -out {FILENAME}
4. Download the error-correcteted data.
    > hadoop fs -getmerge {FILENAME} {FILENAME}.ec.fastq

## Notes
  - The corrected sequences download from HDFS have no order due to the
    distribution natural of Hadoop. If you want to do further experiments in an
    ordered manner, please sort them before analysis.
  - CloudEC is an evaluation and bug fix version of the ReadStackCorrector project.
    You can obtain information on https://github.com/CSCLabTW/ReadStackCorrector/.
