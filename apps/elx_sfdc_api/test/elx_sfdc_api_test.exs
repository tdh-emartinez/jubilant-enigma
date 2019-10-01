defmodule ElxSfdcApiTest do
  use ExUnit.Case
  doctest ElxSfdcApi

  test "greets the world" do
    assert ElxSfdcApi.hello() == :world
  end
end
