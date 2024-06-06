const { execSync } = require("child_process");
const { log } = require("console");

describe("database e2e tests", function () {
  beforeAll(function () {
    // print out the usage of the program.
    // tell the user when to turn on the DEBUG flag.
    const usageMessage = `Normal Usage:
    jest

Turn on debugging with DEBUG=1 like so:
    DEBUG=1 jest
    `;
    log(usageMessage);
  });

  beforeEach(function () {
    execSync("rm -f test.db");
  });

  const runScript = (commands) => {
    let childProcess, rawOutput;

    try {
      let exec_cmd;

      if (process.env.DEBUG === "1") {
        exec_cmd =
          "valgrind --leak-check=full --track-origins=yes ./sqlyte test.db";
      } else {
        exec_cmd = "./sqlyte test.db";
      }

      childProcess = execSync(exec_cmd, {
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
      "lyt-db> need to implement splitting internal node";
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
      "LEAF_NODE_HEADER_SIZE: 14",
      "LEAF_NODE_CELL_SIZE: 297",
      "LEAF_NODE_SPACE_FOR_CELLS: 4082",
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

  it("print all rows in a multi-level tree", function () {
    const commands = [];

    let rows = 15;
    for (let i = 1; i != rows + 1; i++) {
      commands.push(`insert ${i} user${i} person${i}@example.com`);
    }

    commands.push("select", ".exit\n");
    const commandsExpectedResult = [
      "lyt-db> ( 1, user1, person1@example.com )",
      "( 2, user2, person2@example.com )",
      "( 3, user3, person3@example.com )",
      "( 4, user4, person4@example.com )",
      "( 5, user5, person5@example.com )",
      "( 6, user6, person6@example.com )",
      "( 7, user7, person7@example.com )",
      "( 8, user8, person8@example.com )",
      "( 9, user9, person9@example.com )",
      "( 10, user10, person10@example.com )",
      "( 11, user11, person11@example.com )",
      "( 12, user12, person12@example.com )",
      "( 13, user13, person13@example.com )",
      "( 14, user14, person14@example.com )",
      "( 15, user15, person15@example.com )",
      "executed.",
      "lyt-db> ",
    ];

    const result = runScript(commands).slice(-(15 + 2));
    // console.log("result", result);
    expect(result).toStrictEqual(commandsExpectedResult);
  });

  it("allows printing out the structure of a 4-leaf-node btree", function () {
    // input pseudo-randomly generated commands
    const commands = [
      "insert 18 user18 person18@example.com",
      "insert 7 user7 person7@example.com",
      "insert 10 user10 person10@example.com",
      "insert 29 user29 person29@example.com",
      "insert 23 user23 person23@example.com",
      "insert 4 user4 person4@example.com",
      "insert 14 user14 person14@example.com",
      "insert 30 user30 person30@example.com",
      "insert 15 user15 person15@example.com",
      "insert 26 user26 person26@example.com",
      "insert 22 user22 person22@example.com",
      "insert 19 user19 person19@example.com",
      "insert 2 user2 person2@example.com",
      "insert 1 user1 person1@example.com",
      "insert 21 user21 person21@example.com",
      "insert 11 user11 person11@example.com",
      "insert 6 user6 person6@example.com",
      "insert 20 user20 person20@example.com",
      "insert 5 user5 person5@example.com",
      "insert 8 user8 person8@example.com",
      "insert 9 user9 person9@example.com",
      "insert 3 user3 person3@example.com",
      "insert 12 user12 person12@example.com",
      "insert 27 user27 person27@example.com",
      "insert 17 user17 person17@example.com",
      "insert 16 user16 person16@example.com",
      "insert 13 user13 person13@example.com",
      "insert 24 user24 person24@example.com",
      "insert 25 user25 person25@example.com",
      "insert 28 user28 person28@example.com",
      ".btree",
      ".exit\n",
    ];
    const commandsExpectedResult = [
      "lyt-db> tree:",
      "- internal (size 3)",
      " - leaf (size 7)",
      "  - 1",
      "  - 2",
      "  - 3",
      "  - 4",
      "  - 5",
      "  - 6",
      "  - 7",
      " - key 7",
      " - leaf (size 8)",
      "  - 8",
      "  - 9",
      "  - 10",
      "  - 11",
      "  - 12",
      "  - 13",
      "  - 14",
      "  - 15",
      " - key 15",
      " - leaf (size 7)",
      "  - 16",
      "  - 17",
      "  - 18",
      "  - 19",
      "  - 20",
      "  - 21",
      "  - 22",
      " - key 22",
      " - leaf (size 8)",
      "  - 23",
      "  - 24",
      "  - 25",
      "  - 26",
      "  - 27",
      "  - 28",
      "  - 29",
      "  - 30",
      "lyt-db> ",
    ];

    const result = runScript(commands).slice(30);
    expect(result).toStrictEqual(commandsExpectedResult);
  });
});
