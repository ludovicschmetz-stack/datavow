from pathlib import Path

import typer
import yaml

app = typer.Typer(help="datavow — data contract enforcement for modern data teams")


@app.callback()
def main() -> None:
    """datavow — data contract enforcement for modern data teams."""

EXAMPLE_CONTRACT = {
    "contract": "example",
    "description": "Sample data contract",
    "columns": [
        {"name": "id", "type": "integer", "not_null": True},
        {"name": "name", "type": "string", "not_null": True},
        {"name": "created_at", "type": "timestamp"},
    ],
}


@app.command()
def init(
    project_name: str = typer.Argument("my-project", help="Name of the project"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing files"),
) -> None:
    """Scaffold a new datavow project in the current directory."""
    config_path = Path("datavow.yaml")

    if config_path.exists() and not force:
        typer.echo("datavow.yaml already exists. Use --force to overwrite.", err=True)
        raise typer.Exit(code=1)

    config = {
        "project": project_name,
        "connectors": [],
        "rules": [],
    }

    config_path.write_text(yaml.dump(config, sort_keys=False))

    contracts_dir = Path("contracts")
    contracts_dir.mkdir(exist_ok=True)

    example_path = contracts_dir / "example.yaml"
    example_path.write_text(yaml.dump(EXAMPLE_CONTRACT, sort_keys=False))

    typer.echo(f"Initialized datavow project '{project_name}'.")
    typer.echo("Created datavow.yaml and contracts/example.yaml")
