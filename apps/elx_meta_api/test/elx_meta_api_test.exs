defmodule ElxMetaApiTest do
  use ExUnit.Case
  doctest ElxMetaApi

  test "greets the world" do
    assert ElxMetaApi.hello() == :world
  end
end
