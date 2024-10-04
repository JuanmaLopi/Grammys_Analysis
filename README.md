# Grammy Awards Analysis

This project aims to analyze historical data from the Grammy Awards, providing insights into trends, top artists, category evolution, and diversity among nominees. Using Python, Jupyter, Dash, and automation tools like Airflow, the analysis spans multiple aspects of the Grammy Awards, delivering an interactive, data-driven overview.

## Project Motivation

The Grammy Awards have undergone significant evolution since their inception in 1959. This project explores:
- Dominant genres over time.
- Artists with the most awards and nominations.
- Changes in award categories.
- Diversity among nominees and winners.

The goal is to provide a thorough understanding of how the Grammys have shaped and reflected the music industry’s trends and diversity, through reproducible and automated data analysis.

## Key Features

1. **Award Analysis by Genre and Category**: 
   - Historical comparisons of how musical genres (pop, rock, jazz, etc.) are represented among Grammy winners.
   - Exploration of changes in categories and nomination criteria over time.

2. **Artist Analysis**: 
   - Identification of artists with the most wins and nominations, with comparisons across different decades.
   - Insights into emerging and legacy artists' patterns.

3. **Diversity and Representation**:
   - Analysis of gender, ethnic, and national diversity among Grammy nominees and winners.
   - Visualization of representation trends over time, with a focus on major categories.

4. **Dynamic Visualizations**:
   - Interactive dashboard offering customizable views of historical Grammy data.
   - Dashboards for exploring data by artist, category, genre, and more.

5. **Automation with Apache Airflow**:
   - Airflow DAGs to automate data updates and scheduled analysis.
   - Efficient ETL (Extract, Transform, Load) processes for handling large datasets.

## Project Structure

The repository is organized as follows:

- **`Notebooks/`**: Jupyter Notebooks documenting the entire analysis process, from data cleaning to visualization. These notebooks are divided by analysis theme (genre, artist, diversity).
  
- **`dags/`**: Airflow DAGs for automating workflows. The scripts are designed to automate data extraction, cleaning, analysis, and update pipelines.

- **`dashboard/`**: Contains files related to the interactive dashboard built with *Plotly Dash*. The dashboard allows users to explore analysis results interactively.

- **`src/`**: Source code for data extraction, transformation, and analysis functions.

- **`requirements.txt`**: Lists all required dependencies (e.g., *pandas*, *numpy*, *matplotlib*, *plotly*, *dash*, *apache-airflow*).

- **`.gitignore`**: Specifies files and directories to exclude from version control (e.g., virtual environments, temporary data).

## Prerequisites

Ensure you have Python 3.x installed. To install the necessary dependencies, run:

```
pip install -r requirements.txt
```

if you intend to use Apache Airflow, make sure you have it configured in your environment. You can refer to the official installation guide [here](https://airflow.apache.org/docs/apache-airflow/stable/installation.html).

# Installation and Usage
## Cloning the Repository

To use this project in your local environment:

Clone the repository:

```
git clone https://github.com/JuanmaLopi/Grammys_Analysis.git
```

Navigate into the project directory:

```
cd Grammys_Analysis
```

Install the required dependencies:

```
pip install -r requirements.txt
```

# Running Notebooks

To replicate the analysis, open the Jupyter Notebooks located in the Notebooks/ folder. You can launch the Jupyter server by running:

```
jupyter notebook
```

Navigate through the available notebooks and execute the cells to run the analysis and generate visualizations.

# Automating with Airflow

If you plan to automate the analysis, configure and run the DAGs located in the dags/ folder using Apache Airflow. These DAGs automate the extraction, transformation, and updating of data.

# Contribution

Contributions are welcome! If you have ideas to improve the analysis, add new features, or enhance automation, follow these steps:

Fork the repository.
Create a new branch:

```
git checkout -b your-branch-name
```

Make your changes and ensure everything works as expected.
Commit your changes:

```
git commit -m "Description of your changes"
```

Push your changes:

```
git push origin your-branch-name
```

Open a pull request for your changes to be reviewed and merged into the main branch.

# License

This project is licensed under the MIT License, which means you are free to use, modify, and distribute the code under the terms of this license. For more details, see the LICENSE file.

# Contact

- name: Juan Manuel Lopez Rodriguez
- email: [juan_m.lopez_r@uao.edu.co](mailto:tu-email@ejemplo.com)
