defmodule QuiqupElixirKafka.ConsumerSupervisor do
  @moduledoc """
  A supervisor that monitors consumers
  """

  defmodule State do
    @moduledoc false
    defstruct refs: %{}
  end

  use GenServer
  require Logger

  @dynamic_supervisor DynamicSupervisor
  @restart_wait_seconds 1

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(child_specs) do
    import Supervisor.Spec

    {kafka_ex_con_group, _start_link, args} = hd(child_specs).start

    children = [
      supervisor(kafka_ex_con_group, args)
    ]
    Supervisor.start_link(children, strategy: :simple_one_for_one, name: @dynamic_supervisor)

    for child_spec <- child_specs do
      Process.send(self(), {:start_child, child_spec}, [])
    end

    {:ok, %State{}}
  end

  @impl true
  def handle_info({:start_child, child_spec}, %State{refs: refs}) do
    case Supervisor.start_child(@dynamic_supervisor, child_spec) do
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
