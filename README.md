# BDMProject

## Overview

BDMProject is focused on mental health in the context of big data management. The project implements data workflows through the landing, trusted, and exploitation zones, and provides a dashboard to facilitate exploitation tasks and visualization.

## Project Structure

- **Landing Zone:** Initial ingestion of raw data.
- **Trusted Zone:** Processed and validated data.
- **Exploitation Zone:** Data ready for analysis and dashboarding.
- **Dashboard:** Visualization tool for exploitation tasks.

## Repository File Structure

The following is a partial list of files and directories in this repository:
```
exploitation zone/ # with the individual files for the transformations from the trusted to the exploitation zone

hot_path/ # with the files for the streaming

landing zone/ # with the individual files for the ingestion (see part 1 of the project)

orchestration/ # with the files needed for orchestration

tasks/ # with the files for the dashboard display

trusted_zone/ # with the files needed for the transformations from the landing to the trusted zone

```

## Running the Dashboard

To run the dashboard on your local machine, follow these steps:

1. **Clone the Repository**

   ```bash
   git clone https://github.com/vmchristensen/BDMProject.git
   cd BDMProject/tasks
   ```

2. **Prepare the Environment**

   - Ensure the `.env` file (attached in the `learnsql2` task) is present in the same folder as the dashboard code.
   - The `.env` file should contain all necessary environment variables for the dashboard.

3. **Start Docker Services**

   Open a terminal in the command prompt and run:

   ```bash
   docker-compose up -d
   ```

4. **Build the Dashboard Image**

   ```bash
   docker build -t mental-health-dashboard .
   ```

5. **Run the Dashboard Container**

   ```bash
   docker run -p 8501:8501 --env-file .env mental-health-dashboard
   ```

   - If the connection is not working, try changing the port (`8501`) to `8502`, for example:
     ```bash
     docker run -p 8502:8501 --env-file .env mental-health-dashboard
     ```

## Additional Notes

- Make sure Docker is installed and running on your machine.
- The dashboard will be accessible via [http://localhost:8501](http://localhost:8501) (or whichever port you selected).
- If needed contact with us.
