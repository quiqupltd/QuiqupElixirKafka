defmodule QuiqupElixirKafka.Brokers do
  def set_brokers_from_env(env \\ "KAFKA_SERVERS") do
    env |> System.get_env() |> brokers() |> set_env()
  end

  defp brokers(nil), do: nil

  defp brokers(value) do
    value
    |> String.split(",")
    |> Enum.map(fn broker ->
      pieces = String.split(broker, ":")
      {port, _} = Integer.parse(List.last(pieces))
      {List.first(pieces), port}
    end)
  end

  defp set_env(nil), do: nil
  defp set_env(brokers), do: Application.put_env(:kafka_ex, :brokers, brokers)
end
