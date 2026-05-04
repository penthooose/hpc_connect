defmodule HpcConnect.Scripts do
  @moduledoc """
  Locates bundled helper scripts and turns them into remote execution commands.
  """

  alias HpcConnect.{Command, Job, Model, Session, Shell, Slurm, SSH}

  @script_names [
    "build_sif.sh",
    "download_model.sh",
    "export_model_env.sh",
    "find_free_port.sh",
    "start_vllm.sh",
    "vllm_run.sh"
  ]

  @spec local_script_dir() :: binary()
  def local_script_dir do
    :hpc_connect
    |> :code.priv_dir()
    |> to_string()
    |> Path.join("scripts")
  end

  @spec remote_script_dir(Session.t()) :: binary()
  def remote_script_dir(%Session{} = session), do: Path.join(session.work_dir, "scripts")

  @spec install_commands(Session.t()) :: [Command.t()]
  def install_commands(%Session{} = session) do
    remote_root = session.work_dir
    remote_scripts = remote_script_dir(session)

    [
      SSH.ssh_command(
        session,
        "mkdir -p #{Shell.escape(remote_root)} #{Shell.escape(remote_scripts)}",
        "Create remote hpc_connect directories"
      ),
      SSH.scp_to_command(
        session,
        local_script_dir(),
        remote_root,
        "Upload helper scripts to remote host",
        recursive: true
      )
    ]
  end

  def download_model_command(%Session{} = session, %Model{} = model, opts \\ []) do
    script = Path.join(remote_script_dir(session), "download_model.sh")
    target_dir = Model.remote_dir(session, model)
    conda_env = Keyword.get(opts, :conda_env)
    preamble = Slurm.module_load_preamble(Keyword.delete(opts, :conda_env))
    conda_preamble = if conda_env, do: conda_bootstrap_preamble(conda_env), else: ""

    revision_arg =
      case model.revision do
        nil -> ""
        revision -> " --revision #{Shell.escape(revision)}"
      end

    token_arg =
      case model.hf_token_env do
        nil -> ""
        env_name -> " --token-env #{Shell.escape(env_name)}"
      end

    script_command =
      "bash #{Shell.escape(script)} --repo #{Shell.escape(model.repo_id)} --target #{Shell.escape(target_dir)}#{revision_arg}#{token_arg}"

    remote_command =
      [preamble, conda_preamble, script_command]
      |> Enum.reject(&(&1 == ""))
      |> Enum.join(" && ")

    SSH.ssh_command(session, remote_command, "Download model snapshot to remote vault")
  end

  defp conda_bootstrap_preamble(env_name) do
    pkgs_dir = "$HOME/.cache/conda/pkgs"
    envs_dir = "$HOME/.cache/conda/envs"
    env = Shell.escape(env_name)

    [
      "mkdir -p #{pkgs_dir} #{envs_dir}",
      "export CONDA_PKGS_DIRS=#{pkgs_dir}",
      "export CONDA_ENVS_PATH=#{envs_dir}",
      "conda config --append pkgs_dirs #{pkgs_dir} >/dev/null 2>&1 || true",
      "conda config --append envs_dirs #{envs_dir} >/dev/null 2>&1 || true",
      "eval \"$(conda shell.bash hook)\"",
      "if ! conda env list | awk '{print $1}' | grep -qx #{env}; then conda create -n #{env} python=3.12 -y; fi",
      "conda activate #{env}",
      "python -m pip install --user --upgrade huggingface_hub"
    ]
    |> Enum.join(" && ")
  end

  @spec export_env_command(Session.t(), Model.t()) :: Command.t()
  def export_env_command(%Session{} = session, %Model{} = model) do
    script = Path.join(remote_script_dir(session), "export_model_env.sh")
    cache_dir = Model.remote_dir(session, model)

    remote_command = "bash #{Shell.escape(script)} #{Shell.escape(cache_dir)}"
    SSH.ssh_command(session, remote_command, "Render remote model cache exports")
  end

  @spec find_free_port_command(Session.t(), {pos_integer(), pos_integer()}) :: Command.t()
  def find_free_port_command(%Session{} = session, {min_port, max_port}) do
    script = Path.join(remote_script_dir(session), "find_free_port.sh")
    remote_command = "bash #{Shell.escape(script)} #{min_port} #{max_port}"
    SSH.ssh_command(session, remote_command, "Find a free remote TCP port")
  end

  @spec start_vllm_command(Session.t(), Model.t(), Job.t()) :: Command.t()
  def start_vllm_command(%Session{} = session, %Model{} = model, %Job{} = job) do
    script = Path.join(remote_script_dir(session), "start_vllm.sh")
    model_dir = Model.remote_dir(session, model)
    env_exports = Model.env_exports(session, model)
    port = job.port || elem(job.port_range, 0)

    module_prefix =
      case job.modules do
        [] -> ""
        modules -> Enum.map_join(modules, " && ", &"module load #{&1}") <> " && "
      end

    conda_prefix =
      case job.conda_env do
        nil ->
          ""

        env_name ->
          "source ~/.bashrc >/dev/null 2>&1 || true && conda activate #{env_name} && "
      end

    extra_args = Enum.map_join(job.extra_args, " ", &Shell.escape/1)

    remote_command =
      "#{module_prefix}#{conda_prefix}#{env_exports} bash #{Shell.escape(script)} --model #{Shell.escape(model_dir)} --host #{Shell.escape(job.vllm_host)} --port #{port}" <>
        if(extra_args == "", do: "", else: " -- #{extra_args}")

    SSH.ssh_command(session, remote_command, "Start remote vLLM server")
  end

  @spec script_names() :: [binary()]
  def script_names, do: @script_names
end
