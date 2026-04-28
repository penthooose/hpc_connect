defmodule HpcConnect.EnvFile do
  @moduledoc """
  Minimal `.env` reader for local developer workflows.

  This module intentionally supports the common subset we need here:
  - blank lines
  - comments starting with `#`
  - `KEY=value`
  - optional `export KEY=value`
  - quoted or unquoted values
  """

  @spec load(binary()) :: map()
  def load(path) when is_binary(path) do
    cond do
      File.exists?(path) ->
        path |> File.read!() |> parse()

      path == ".env" ->
        example = ".env.example"

        if File.exists?(example) do
          example |> File.read!() |> parse()
        else
          %{}
        end

      true ->
        %{}
    end
  end

  @spec parse(binary()) :: map()
  def parse(content) when is_binary(content) do
    content
    |> String.split("\n")
    |> Enum.reduce(%{}, fn line, acc ->
      case parse_line(line) do
        nil -> acc
        {key, value} -> Map.put(acc, key, value)
      end
    end)
  end

  defp parse_line(line) do
    trimmed = String.trim(line)

    cond do
      trimmed == "" ->
        nil

      String.starts_with?(trimmed, "#") ->
        nil

      true ->
        trimmed
        |> String.trim_leading("export ")
        |> split_assignment()
    end
  end

  defp split_assignment(line) do
    case String.split(line, "=", parts: 2) do
      [key, value] ->
        {String.trim(key), normalize_value(String.trim(value))}

      _ ->
        nil
    end
  end

  defp normalize_value("\"" <> rest), do: strip_matching_suffix(rest, "\"")
  defp normalize_value("'" <> rest), do: strip_matching_suffix(rest, "'")
  defp normalize_value(value), do: value

  defp strip_matching_suffix(value, suffix) do
    String.trim_trailing(value, suffix)
  end
end
