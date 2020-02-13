# Run this app from Neotomex root with
#
#     mix run -r examples/etl.exs

defmodule ETL do
  use Neotomex.ExGrammar

  alias Neotomex.ExGrammar

  @root true
  define :json_value,
      "space? (object / integer_array / array / number_range / number / emptyString / notEmptyString / true / false / null / function )? space?" do
    [_, json_value, _] -> json_value #|> IO.inspect(label: "json_value")
  end

  define :object,
      "<'{'> <space?> pair (<space?> <','> <space?> pair)* <space?> <'}'> / <'{'> <space?> <'}'>" do
    [] -> Map.new()
    [head, others] ->
      Enum.into([head | (for [pair] <- others, do: pair)], Map.new())
  end

  define :pair, "<space?> (string / keyword) <space?> <':'> <space?> json_value <space?>" do
    [string, val] -> {String.to_atom(string), val}
  end

  define :string, "(emptyString / notEmptyString)" do
    str -> str
  end

  define :emptyString, "<'\"'> <'\"'>" do
    [] -> ""
    [chars] -> chars |> Enum.join #|> IO.inspect(label: "emptyString")
  end

  # TODO - not properly matching escape seqs
  define :notEmptyString, "<'\"'>  (<!'\"'> ((qq / cr / newline) / .))+ <'\"'>" do
    [chars] -> chars |> Enum.join
  end

  define :keyword, "letter letterFollow+" do
    [head, tail] ->
      [ head | tail ] |> Enum.join
  end

  define :function, "keyword (<'.'> keyword)? <'('> <space?> object? <space?> <')'>" do
    [ object, nil, nil ] -> %{ functionCall: %{ object: object, method: nil, arguments: %{} } }
    [ object, nil, tail ] -> %{ functionCall: %{ object: object, method: nil, arguments: tail } }
    [ object, [method], tail ] -> %{ functionCall: %{ object: object, method: method, arguments: tail } }
  end

  @spec flatten(list) :: list
  def flatten([]), do: [] #|> IO.inspect(label: "1")
  def flatten([nil | rest]), do: flatten(rest) #|> IO.inspect(label: "2")
  def flatten([head | rest]) when is_list(head), do: flatten(head) ++ flatten(rest) #|> IO.inspect(label: "3")
  def flatten([head | rest]), do: [head | flatten(rest)] #|> IO.inspect(label: "4")
  def flatten(var), do: [var] #|> IO.inspect(label: "1")

  define :integer_array, "<'['> <space?> (number_range / int+) (<space?> <','> <space?> (number_range / int+))* <space?> <']'> / <'['> space? <']'>" do
    [] -> []
    [[]] -> []
    [head, tail] -> [ head | tail ]
            #|> IO.inspect(label: "before flatten")
            |> flatten()
            #|> IO.inspect(label: "before to_integer")
            |> Enum.map(&to_string/1)
            |> Enum.map(&String.to_integer/1)
            #|> IO.inspect(label: "flat integer array")
  end

  define :array, "<'['> <space?> json_value (<space?> <','> <space?> json_value)* <space?> <']'> / <'['> space? <']'>" do
    [] -> []
    [[]] -> []
    [head, rest] -> [head | (for [val] <- rest, do: val)] #|> IO.inspect(label: "array")
  end

  define :number, "int frac? exp?" do
    [int, nil,  nil] -> iolist_to_integer(int)
    [int, frac, exp] ->
      base = if frac do
               iolist_to_float([int | frac])
             else
               iolist_to_integer(int)
             end
      base = if exp, do: base * :math.pow(10, exp), else: base
      if frac, do: base, else: round(base)
  end

  define :int, "'-'? (non_zero_digit digit+) / digit" do
    [nil, [head, rest]]         -> [head | rest] |> Enum.join #|> IO.inspect(label: "int1")
    ["-", [head, rest]]         -> [head | rest] |> Enum.join |> (fn(s) -> "-#{s}" end).() #|> IO.inspect(label: "int1")
    digit when is_binary(digit) -> [digit] #|> IO.inspect(label: "int2")
  end

  # Produce the exponent as an integer
  define :exp, "e digit+" do
    [suffix, digits] -> iolist_to_integer([suffix | digits])
  end

  define :e, "<[eE]> ('+' / '-')?" do
    [nil]    -> "+"
    [suffix] -> suffix
  end

  define :lc_start,       "'//'"
  define :number_range,   "digit+ <'..'> digit+", do: (
      [[start], [stop]] -> Enum.into(String.to_integer(start)..String.to_integer(stop), []) # |> IO.inspect(label: "number_range"))
      [start, stop] -> Enum.into(String.to_integer(start |> Enum.join |> to_string)..String.to_integer(stop |> Enum.join |> to_string), [])
    ) # |> IO.inspect(label: "number_range"))
  define :newline,        "[\\n]", do: (_ -> "\n" ), do: (any -> any) # |> IO.inspect(label: "newline"))
  define :cr,             "'\\r'", do: (_ -> "\r"), do: (any -> any) # |> IO.inspect(label: "cr"))
  define :tab,            "'\\t'", do: (_ -> "\t"), do: (any -> any) # |> IO.inspect(label: "tab"))
  define :qq,             "'\\\"'", do: (_ -> "&quot;"), do: (any -> any) # |> IO.inspect(label: "qq"))
  define :backslash,      "'\\\\'", do: (_ -> "\\"), do: (any -> any ) # |> IO.inspect(label: "backslash"))
  define :frac,           "'.' digit+",  do: ([head, rest] -> [head | rest])
  define :non_zero_digit, "[1-9]"
  define :digit,          "[0-9]"
  define :letter,         "[a-zA-Z]"
  define :letterFollow,   "[a-zA-Z0-9_]"
  define :true,           "'true'",      do: (_ -> true)
  define :false,          "'false'",     do: (_ -> false)
  define :null,           "'null'",      do: (_ -> nil)
  define :space,          "[ \\r\\n\\s\\t]*"
  define :quote,          "'&quot;'", do: (_ -> "\"")
  define :notnewline,     "[^\\n]"

  defp iolist_to_integer(digits) do
    digits |> :erlang.iolist_to_binary |> String.to_integer
  end

  defp iolist_to_float(digits) do
    digits |> :erlang.iolist_to_binary |> String.to_float
  end

  @doc """
  ETL parsing REPL based on json.exs from neotomex
  """
  def repl do
    input = IO.gets "Enter a valid ETL/JSON expression: "
    case input |> String.trim |> parse do
      {:ok, result} ->
        result |> IO.inspect(label: "result")
      {:ok, result, remainder} ->
        result |> IO.inspect(label: "result")
        remainder |> IO.inspect(label: "remainder")
      :mismatch ->
        IO.puts "You sure you got that right?"
    end
    repl()
  end

  def safeParse(value, extra \\ %{}) do
    value = Regex.replace(~r/\\"/, value, "&quot;")
    value = Regex.replace(~r{//[^\n]+\n}, value, "")
    value = Regex.replace(~r{/\*.*\*/}U, value, "")
    value = Regex.replace(~r{[\r\n]}, value, "")
    value = Regex.replace(~r/{([a-zA-Z0-9_.]+)}/, value, fn _, x -> "\"#{extra[String.to_atom(x)]}\"" end)
    parse(String.trim(value))
  end

  @doc """
  Basic JSON/ETL parsing tests.
  """
  def test do
    {:ok, 1} = safeParse("1") #|> IO.inspect(label: "number")
    {:ok, 3000} = safeParse("3e3")
    {:ok, 3.0e3} = safeParse("3.0e3")
    {:ok, 3.3} = safeParse("3.3")
    {:ok, [1]} = safeParse("[1]") #|> IO.inspect(label: "single element array")
    {:ok, [1,2]} = safeParse("[1,2]") #|> IO.inspect(label: "multi element array")
    {:ok, [1,[1,2]]} = safeParse("[1,[1,2]]") #|> IO.inspect(label: "multi type multi element array")
    {:ok, [5, 6, 7, 8, 9]} = safeParse("[ 5..9 ]") #|> IO.inspect(label: "range1")
    {:ok, [0, 1, 1, -555, 5, 6, 7, 8, 9, 4, 20]} = safeParse("[0,1,1,-555,5..9,4,20]") #|> IO.inspect(label: "range2")

    {:ok, "abc&quot;def"} = safeParse("\"abc\\\"def\"") #|> IO.inspect(label: "escaped quote string")
    dict = [a: 1] |> Enum.into(Map.new)
    {:ok, ^dict} = safeParse("{\"a\": 1}") #|> IO.inspect(label: "simple object")
    {:ok, nil} = safeParse("//Test\n") #|> IO.inspect(label: "Single Line Comment")
    {:ok, %{ key: _value }} = safeParse("{ key: \"value\" }") #|> IO.inspect(label: "Simple Hash")
    {:ok, %{key: %{functionCall: %{arguments: %{}, method: nil, object: "Function"}}}} = safeParse("{ key: Function() }") #|> IO.inspect(label: "Simple Function 1")
    {:ok, %{key: %{functionCall: %{arguments: %{}, method: nil, object: "Function"}}}} = safeParse("{ key: Function({}) }") #|> IO.inspect(label: "Simple Function 2")
    {:ok, %{key: [%{functionCall: %{arguments: %{key: 1, key2: "2"}, method: nil, object: "Function"}}]}} = safeParse("{ key: [ Function({key: 1, key2: \"2\"}) ] }") #|> IO.inspect(label: "Function with Arguments")
    {:ok, %{key: [%{functionCall: %{arguments: %{key: 1, key2: "2"}, method: "method", object: "Function"}}]}} = safeParse("{ key: [ Function.method({key: 1, key2: \"2\"}) ] }") #|> IO.inspect(label: "Function with Arguments and Method")
    {:ok, %{addressMongo: [%{functionCall: %{arguments: %{indexMap: %{city: 2, recordID: 0, state: 3, street: 1, zip: 4, "zip+4": 5}}, method: "save", object: "addressTable"}}], define: %{addressTable: %{functionCall: %{arguments: %{collection: "address", url: "mongodb://localhost/mycollection"}, method: nil, object: "MongoCollection"}}, nameTable: %{functionCall: %{arguments: %{collection: "name", url: "mongodb://localhost/mycollection"}, method: nil, object: "MongoCollection"}}}, nameMongo: [%{functionCall: %{arguments: %{indexMap: %{firstName: 2, lastName: 1, middleName: 3, nickName: 2, recordID: 0}}, method: "save", object: "nameTable"}}], start: [%{functionCall: %{arguments: %{}, method: nil, object: "DefaultStream"}}, %{functionCall: %{arguments: %{}, method: nil, object: "TextToLines"}}, %{functionCall: %{arguments: %{canBeEnclosedBy: "&quot;", delimiter: ","}, method: nil, object: "DelimitedRecordToArray"}}, %{functionCall: %{arguments: %{head: true}, method: nil, object: "GenerateUUID"}}, %{functionCall: %{arguments: %{targets: [%{dest: "nameMongo", fields: [0, 1, 2, 3, 4]}, %{dest: "addressMongo", fields: [0, 5, 6, 7, 8, 9]}]}, method: nil, object: "SendFieldsToBranch"}}]}}
        = safeParse(File.read!("test/sample2.js"), %{mongoURL: "mongodb://localhost/mycollection"}) #|> IO.inspect(label: "Full Script")
  end
end

ETL.validate
ETL.test

IO.puts "A JSON/ETL Parser. See this file for the implementation."
IO.puts "====================================================\n"
ETL.repl
