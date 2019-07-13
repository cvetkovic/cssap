#!/bin/bash
echo "InterpolationTopology - simulation"
   
for i in {1..25}
do
	echo "Simulation No. $i"
	      
	storm jar /mnt/c/Users/cl160127d/IdeaProjects/apache_storm/target/apache_storm-1.0.jar cvetkovic.Topology
	         
	sleep 60
	echo "----------------------------------------------------"
	storm kill InterpolationTopology -w 2
	sleep 7

	mv /home/cvetkovic/performance.txt /home/cvetkovic/performance$i.txt -f
			        
done

echo "InterpolationTopology - simulation done"
echo "---------------------------------------"
