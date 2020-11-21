# MyName Search Engine

# Requirements
[x] First Java Application  Implementation  and Execution on Docker
[x] Docker to Local (or GCP) Cluster Communication
[x] Inverted Indexing MapReduce Implementation  and Execution on the Cluster  (GCP)
[x] Term and Top-N  Search

# Getting Started

## Prerequisites

Docker : https://hub.docker.com/editions/community/docker-ce-desktop-windows/ 

XMing (for GUi) : XMing : http://www.straightrunning.com/XmingNotes/ 
    1. Configure X0.hosts in C:/Program Files (x86)/Xming/X0.hosts or whichever folder the folder is in  
    2. Add IP address of remote host (I had 192.168.56.1) 
    3. Save

## Pulling From Docker

'docker pull qic28/searchengine'

## Running Program

'docker run -e DISPLAY=192.168.56.1:0.0 qic28/searchengine'  

or  

'docker run -e DISPLAY=<IP address added to X0.hosts> qic28/searchengine'

## Video

