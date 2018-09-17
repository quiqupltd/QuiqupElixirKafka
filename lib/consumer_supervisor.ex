defmodule QuiqupElixirKafka.ConsumerSupervisor do
  @moduledoc """
  A supervisor that monitors consumers
  """

  defmodule State do
    @moduledoc false
    defstruct refs: %{}, dynamic_supervisor?: false
  end

  use GenServer
  require Logger

  @dynamic_supervisor DynamicSupervisor
  @restart_wait_seconds 1

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(child_specs) do
    dynamic_supervisor_present()
    |> version_init(child_specs)
  end

  defp dynamic_supervisor_present() do
    case function_exported?(DynamicSupervisor, :__info__, 1) do
      true -> :has_dynamic_supervisor
      false -> :without_dynamic_supervisor
    end
  end

  defp version_init(:without_dynamic_supervisor, child_specs)
    import Supervisor.Spec

    {kafka_ex_con_group, _start_link, args} = hd(child_specs).start

    children = [
      supervisor(kafka_ex_con_group, args, restart: :transient)
    ]
    Supervisor.start_link(children, strategy: :simple_one_for_one, name: @dynamic_supervisor)

    for child_spec <- child_specs do
      Process.send(self(), {:start_child, child_spec}, [])
    end

    {:ok, %State{}}
  end

  defp version_init(:has_dynamic_supervisor, child_specs) do
    DynamicSupervisor.start_link(strategy: :one_for_one, name: @dynamic_supervisor)

    for child_spec <- child_specs do
      Process.send(self(), {:start_child, child_spec}, [])
    end

    {:ok, %State{dynamic_supervisor?: true}}
  end

  @impl true
  def handle_info({:start_child, child_spec}, %State{refs: refs, dynamic_supervisor?: dynamic_supervisor?}) do
    Logger.info("#{__MODULE__} Starting child consumer:#{inspect(child_spec)}")
    case version_case(dynamic_supervisor?, child_spec) do
      {:ok, pid} ->
        Logger.info("#{__MODULE__} monitoring #{inspect(child_spec)} at #{inspect(pid)}")
        ref = Process.monitor(pid)
        refs = Map.put(refs, ref, child_spec)

        {:noreply, %State{refs: refs}}

      {:error, error} ->
        Logger.warn(
          "#{__MODULE__} #{inspect(child_spec)} failed to start #{inspect(error)}. Restarting in #{
            @restart_wait_seconds
          } seconds..."
        )

        Process.send_after(self(), {:start_child, child_spec}, @restart_wait_seconds * 1000)

        {:noreply, %State{refs: refs}}
    end
  end

  defp version_case(false, _child_spec), do: Supervisor.start_child(@dynamic_supervisor, [])
  defp version_case(true, child_spec), do: DynamicSupervisor.start_child(@dynamic_supervisor, child_spec)

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{refs: refs}) do
    Logger.warn("#{__MODULE__}.handle_info :DOWN ref:#{inspect(ref)}")

    {child_spec, refs} = Map.pop(refs, ref)

    Logger.warn(
      "KafkaEx #{inspect(child_spec)} went down. Restarting in #{@restart_wait_seconds} seconds..."
    )

    Process.send_after(self(), {:start_child, child_spec}, @restart_wait_seconds * 1000)

    {:noreply, %State{refs: refs}}
  end
end
