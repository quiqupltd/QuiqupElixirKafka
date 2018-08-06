defmodule QuiqupElixirKafka.KafkaMonitor do
  @moduledoc """
  A genserver that monitors kafka brokers,
  if they go down it attempts reconnections
  """
  use GenServer
  require Logger

  @restart_wait_seconds 1

  defmodule State do
    @moduledoc false
    defstruct worker_ref: nil
  end

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl true
  def init(:ok) do
    Process.send(self(), :start_worker, [])

    {:ok, %State{}}
  end

  @impl true
  def handle_info(:start_worker, %State{worker_ref: _worker_ref}) do
    Application.start(:kafka_ex)

    case KafkaEx.create_worker(:kafka_ex, consumer_group: :no_consumer_group) do
      {:ok, pid} ->
        Logger.info("#{__MODULE__} Monitoring KafkaEx worker at " <> inspect(pid))
        Process.monitor(pid)

        {:noreply, %State{worker_ref: pid}}

      {:error, {:already_started, pid}} ->
        Logger.info("#{__MODULE__} KafkaEx worker already running at " <> inspect(pid))
        Process.monitor(pid)

        {:noreply, %State{worker_ref: pid}}

      {:error, error} ->
        Logger.warn(
          "#{__MODULE__} KafkaEx worker failed to start. Restarting in #{@restart_wait_seconds} seconds..."
        )

        Logger.debug(inspect(error))
        Process.send_after(self(), :start_worker, @restart_wait_seconds * 1000)

        {:noreply, %State{worker_ref: nil}}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, _reason}, %{worker_ref: worker_ref}) do
    Logger.warn(
      "#{__MODULE__} KafkaEx worker went down. Restarting in #{@restart_wait_seconds} seconds..."
    )

    Process.send_after(self(), :start_worker, @restart_wait_seconds * 1000)
    {:noreply, %State{worker_ref: worker_ref}}
  end
end
