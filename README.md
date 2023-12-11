# 📊 SEC Report 13F

This project is a quick implementation to scrap the SEC 13F reports and store their information in a database. Since the response times of the website where very slow, the IDs of the reports were first stored in a database by using an Azure Durable Function orchestrator with a queue trigger and then reading the reports one at a time based on the ID by using a timer trigger.

## ⚙️ Functionality

The process consists of web-scraping the SEC website with the filter on the 13F reports to store the information into the database. There are multiple hacks to run the whole thing properly, since the method is far from ideal and the strcuture of the reports changes over time. Sometimes, information is missing and this leads to issues.

IDs that had issues with the queue trigger are stored as well and retrieved at a later stage with the pending ones, so no report goes lost.

## 📜 Notes

This was part of a larger project that never went into production, so a cleaner implementation with a more TDD approach won't happen.

The project is being made public without the git history.