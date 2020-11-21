# MyName Search Engine

# Requirements
- First Java Application  Implementation  and Execution on Docker
- Docker to Local (or GCP) Cluster Communication
- Inverted Indexing MapReduce Implementation  and Execution on the Cluster  (GCP)
- Term and Top-N  Search

# Getting Started

## Prerequisites
You will need to download both Docker and XMing onto your machine.

Docker : https://hub.docker.com/editions/community/docker-ce-desktop-windows/ 

XMing (for GUi) : XMing : http://www.straightrunning.com/XmingNotes/ 
    1. Configure X0.hosts in C:/Program Files (x86)/Xming/X0.hosts or whichever folder the folder is in  
    2. Add IP address of remote host (I had 192.168.56.1) 
    3. Save

## Pulling From Docker

Pull from Docker repository using command


`docker pull qic28/searchengine`


## Running Program

`docker run -e DISPLAY=192.168.56.1:0.0 qic28/searchengine`  

or  

`docker run -e DISPLAY=<IP address added to X0.hosts> qic28/searchengine`


## Video

### [OneDrive](https://pitt-my.sharepoint.com/:v:/g/personal/qic28_pitt_edu/Edz_srMLajhNtIruI0ynQ-oB69vstH2ARLYGPLr_aeBoFA?e=BaZMgO)

### [Youtube Link](https://youtu.be/67K2fMPjf80)
