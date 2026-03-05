from typer.testing import CliRunner

from datavow.cli import app

runner = CliRunner()


def test_init_creates_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "test-project"])
    assert result.exit_code == 0
    assert (tmp_path / "datavow.yaml").exists()
    assert (tmp_path / "contracts" / "example.yaml").exists()
    assert "test-project" in (tmp_path / "datavow.yaml").read_text()


def test_init_refuses_overwrite(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "datavow.yaml").write_text("existing")
    result = runner.invoke(app, ["init"])
    assert result.exit_code == 1
    assert "already exists" in result.output


def test_init_force_overwrites(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "datavow.yaml").write_text("existing")
    result = runner.invoke(app, ["init", "--force"])
    assert result.exit_code == 0
    assert "my-project" in (tmp_path / "datavow.yaml").read_text()
