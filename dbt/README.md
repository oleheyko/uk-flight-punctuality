Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test

### Using Docker

A container image is provided for this dbt project.

Build the image from the `dbt/` directory:

```bash
cd dbt
docker build -t uk-flight-dbt .
```

Run dbt inside the container using the baked-in `profiles.yml` and the repository service account key:

```bash
cd dbt
docker run --rm \
  -v "$PWD":/usr/app \
  -v "../keys/my-creds.json":/tmp/credentials.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/credentials.json \
  uk-flight-dbt run
```

> Note: the image already defines `ENTRYPOINT ["dbt"]`, so you should pass only the dbt subcommand like `run`.
>
> Do not mount your host `~/.dbt` into the container. The image already contains its own `profiles.yml` and the container profile points at `/tmp/credentials.json`.

Or use Docker Compose:

```bash
cd dbt
docker compose run --rm dbt run
```

The `dbt/docker-compose.yml` service already mounts `../keys/my-creds.json` into `/tmp/credentials.json` and sets `GOOGLE_APPLICATION_CREDENTIALS=/tmp/credentials.json` inside the container.
### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
