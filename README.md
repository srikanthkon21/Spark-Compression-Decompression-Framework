# Spark-Compression-Decompression-Framework
To Compress and Decompress Data in Data-lake (Hadoop)

#UserStory
Compression and Decompression of files to save the space on nodes and Speed up processingusingSparkEngine.

#Introduction
File compression brings two major benefits it reduces the space needed to store files, and it speeds up data transfer. When dealing with large volumes of data, both of these savings can be significant, so it pays to carefully consider how to use compression in Hadoop
Compression reduces the size of an application or document for storage. Compressed files are smaller,download faster, and easier to transport. Decompression restoresthedocumentorapplicationtoitsoriginalsize.

#Compression/DecompressionTechniqueUsed
GZIP compression uses more CPU resourcesthan Snappy orLZO, but providesa higher compression ratio.GZip is often a good choice for cold data, which is accessed infrequently. Snappy or LZO are a better choice for hot data, which is accessed frequently.

#TechnicalSpecification
 Scala 
 Spark 
 GZipCompression 
 ShellScripting 
 HDFS

#Compressing contents has some advantages and disavantages Advantages
 Compressing contents helps with decreasing the time it will take for client to download. It saves your bandwidth so it reduces costs in many countries monthly bandwidthsaretooexpensiveandthisisimportant tosaveit.  Compressed data can save storage space and speed up data transfers across the network  A compressed file requires less storage capacity than an uncompressed file, and the use of compression can lead to a significant decrease in expenses. A compressed file also requires less time for transfer, and it consumes less network bandwidththananuncompressedfile.

Disadvantages
 Compressingcontentseatsyourserver’sCPUcycles!

#Gzipcodecarenotsplittable
Files compressed with the gzip codec are not splittable due to the nature of the codec.This limits the options you have scaling out when reading large gzipped input files.Given the fact that gunzipping a 1GiB file usually takes only 2 minutes I figured that for some use cases wasting some resources may result in a shorter job time under certain conditions.So reading the entire input file from the start for each split (wasting resources!!)may leadtoadditional scalability.
