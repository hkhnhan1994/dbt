Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt deps
- dbt run
- dbt test

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### change profile dir:
In your prompt type: 
- export DBT_PROFILES_DIR=/path/to/folder

This command will reassign the location of your profiles.yml to the folder mentioned in /path/to/folder.

Once it is done, run: 
- dbt debug --config-dir

### generate graph

- dbt docs generate
- dbt docs serve