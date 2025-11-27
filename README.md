# github-flutter-scraper
# Flutter GitHub Scraper

This repository contains a simple Python script I wrote to download complete Flutter project repositories from GitHub.  
I built this mainly for creating datasets and experimenting with automation workflows.


## What the script does
- Searches GitHub for Flutter repositories based on keywords  
- Downloads the entire repository  
- Picks out useful files (like `lib/`, `pubspec.yaml`, etc.)  
- Organizes everything into a clean folder structure  

It’s not perfect, but it gets the job done and is helpful for dataset preparation.


## Output structure
The downloaded projects are arranged like this:

dataset/
  project_1/
    lib/
    pubspec.yaml
  project_2/
    lib/
    pubspec.yaml


## How to use
Use the following commands:
```
pip install -r requirements.txt
python scraper.py
```


## Files
- scraper.py — main script  
- README.md — documentation  



## Notes
This project is still a work in progress. I'll keep improving it as I refine my workflow.
