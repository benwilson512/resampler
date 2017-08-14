defmodule Resampler do

  def eager(ts, width) do
    ts
    |> Bucket.eager(width)
  end

  def lazy(ts, width) do
    ts
    |> Bucket.lazy(width)
  end

end
