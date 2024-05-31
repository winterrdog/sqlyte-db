const { execSync } = require("child_process");

describe("database e2e tests", function () {
  beforeEach(function () {
    execSync("rm -f test.db");
  });

  const runScript = (commands) => {
    let childProcess, rawOutput;

    try {
      childProcess = execSync("./sqlyte test.db", {
        input: commands.join("\n"),
      });
      rawOutput = childProcess.toString();
    } catch (e) {
      rawOutput = e.stdout.toString();
    }

    return rawOutput.split("\n");
  };

  it("inserts and retrieves a row", function () {
    const commands = [
      "insert 1 user1 person1@example.com",
      "select",
      ".exit\n",
    ];
    const commandsExpectedResult = [
      "lyt-db> executed.",
      "lyt-db> ( 1, user1, person1@example.com )",
      "executed.",
      "lyt-db> ",
    ];
    const result = runScript(commands);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it("prints error message when table's full", function () {
    const createManyEntries = () => {
      let i;
      const result = [];
      const maxTableRows = 1400;

      for (i = 0; i != maxTableRows + 0x1; i++) {
        result.push(`insert ${i} user${i} person${i}@example.com`);
      }

      result.push(".btree\n");
      result.push(".exit\n");
      return result;
    };

    const commands = createManyEntries();
    const commandsExpectedResult =
      "lyt-db> need to implement updating parent after splitting";
    const result = runScript(commands);
    expect(result.at(-2)).toEqual(commandsExpectedResult);
  });

  it("allows inserting strings that are the maximum length", function () {
    const longUsername = "w".repeat(32);
    const longEmail = "w".repeat(255);
    const commands = [
      `insert 1 ${longUsername} ${longEmail}`,
      "select",
      ".exit\n",
    ];
    const commandsExpectedResult = [
      "lyt-db> executed.",
      `lyt-db> ( 1, ${longUsername}, ${longEmail} )`,
      "executed.",
      "lyt-db> ",
    ];
    const result = runScript(commands);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it("prints error message if strings are too long", function () {
    const longUsername = "w".repeat(33);
    const longEmail = "w".repeat(256);
    const commands = [
      `insert 1 ${longUsername} ${longEmail}`,
      "select",
      ".exit\n",
    ];
    const commandsExpectedResult = [
      "lyt-db> string is too long.",
      "lyt-db> executed.",
      "lyt-db> ",
    ];
    const result = runScript(commands);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it('prints an error message if "id" is negative', function () {
    const commands = ["insert -1 cstack foo@bar.com", "select", ".exit\n"];
    const commandsExpectedResult = [
      "lyt-db> id must be non-negative.",
      "lyt-db> executed.",
      "lyt-db> ",
    ];
    const result = runScript(commands);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it("keeps data even after closing connection", function () {
    {
      const commands = ["insert 1 user1 person1@example.com", ".exit\n"];
      const commandsExpectedResult = ["lyt-db> executed.", "lyt-db> "];
      const result = runScript(commands);
      expect(result).toStrictEqual(commandsExpectedResult);
    }

    // reopen program after an exit
    {
      const commands = ["select", ".exit\n"];
      const commandsExpectedResult = [
        "lyt-db> ( 1, user1, person1@example.com )",
        "executed.",
        "lyt-db> ",
      ];
      const result = runScript(commands);
      expect(result).toStrictEqual(commandsExpectedResult);
    }
  });

  it("inserts and retrieves 3 rows", function () {
    const commands = [
      "insert 1 user1 person1@example.com",
      "insert 2 user2 person2@example.com",
      "insert 3 user3 person3@example.com",
      "select",
      ".exit\n",
    ];
    const commandsExpectedResult = [
      "lyt-db> executed.",
      "lyt-db> executed.",
      "lyt-db> executed.",
      "lyt-db> ( 1, user1, person1@example.com )",
      "( 2, user2, person2@example.com )",
      "( 3, user3, person3@example.com )",
      "executed.",
      "lyt-db> ",
    ];
    const result = runScript(commands);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it("inserts and retrieves 3 rows after reopening", function () {
    {
      const commands = [
        "insert 1 user1 person1@example.com",
        "insert 2 user2 person2@example.com",
        "insert 3 user3 person3@example.com",
        ".exit\n",
      ];
      const commandsExpectedResult = [
        "lyt-db> executed.",
        "lyt-db> executed.",
        "lyt-db> executed.",
        "lyt-db> ",
      ];
      const result = runScript(commands);
      expect(result).toStrictEqual(commandsExpectedResult);
    }

    // reopen program after an exit
    {
      const commands = ["select", ".exit\n"];
      const commandsExpectedResult = [
        "lyt-db> ( 1, user1, person1@example.com )",
        "( 2, user2, person2@example.com )",
        "( 3, user3, person3@example.com )",
        "executed.",
        "lyt-db> ",
      ];
      const result = runScript(commands);
      expect(result).toStrictEqual(commandsExpectedResult);
    }
  });

  it("allows printing out the structure of a one-node btree", function () {
    const commands = [
      "insert 3 user3 person3@example.com",
      "insert 1 user1 person1@example.com",
      "insert 2 user2 person2@example.com",
      ".btree",
      ".exit\n",
    ];
    const commandsExpectedResult = [
      "lyt-db> executed.",
      "lyt-db> executed.",
      "lyt-db> executed.",
      "lyt-db> tree:",
      "- leaf (size 3)",
      " - 1",
      " - 2",
      " - 3",
      "lyt-db> ",
    ];
    const result = runScript(commands);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it("prints constants", function () {
    const commands = [".constants", ".exit\n"];
    const commandsExpectedResult = [
      "lyt-db> constants:",
      "ROW_SIZE: 293",
      "COMMON_NODE_HEADER_SIZE: 6",
      "LEAF_NODE_HEADER_SIZE: 10",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4086",
      "LEAF_NODE_MAX_CELLS: 13",
      "lyt-db> ",
    ];
    const result = runScript(commands);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it('prints an error message if "id" is a duplicate', function () {
    const commands = [
      "insert 1 user1 person1@example.com",
      "insert 1 user1 person1@example.com",
      "select",
      ".exit\n",
    ];
    const commandsExpectedResult = [
      "lyt-db> executed.",
      "lyt-db> error: duplicate key.",
      "lyt-db> ( 1, user1, person1@example.com )",
      "executed.",
      "lyt-db> ",
    ];

    const result = runScript(commands);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it("allows printing out the structure of a 3 leaf-node btree", function () {
    const commands = [];
    let rows = 15;
    for (let i = 1; i != rows; i++) {
      commands.push(`insert ${i} user${i} person${i}@example.com`);
    }

    commands.push(".btree", "insert 15 user15 person15@example.com", ".exit\n");
    const commandsExpectedResult = [
      "lyt-db> tree:",
      "- internal (size 1)",
      " - leaf (size 7)",
      "  - 1",
      "  - 2",
      "  - 3",
      "  - 4",
      "  - 5",
      "  - 6",
      "  - 7",
      " - key 7",
      " - leaf (size 7)",
      "  - 8",
      "  - 9",
      "  - 10",
      "  - 11",
      "  - 12",
      "  - 13",
      "  - 14",
      "lyt-db> executed.",
      "lyt-db> ",
    ];

    const result = runScript(commands).slice(14);
    expect(result).toStrictEqual(commandsExpectedResult);
  });
});
