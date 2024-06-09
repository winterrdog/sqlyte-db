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
      "lyt-db> tried to fetch a page out of bounds. 101 > 100";

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

  it("allows printing out the structure of a 7-leaf-node btree", function () {
    const commands = [
      "insert 58 user58 person58@example.com",
      "insert 56 user56 person56@example.com",
      "insert 8 user8 person8@example.com",
      "insert 54 user54 person54@example.com",
      "insert 77 user77 person77@example.com",
      "insert 7 user7 person7@example.com",
      "insert 25 user25 person25@example.com",
      "insert 71 user71 person71@example.com",
      "insert 13 user13 person13@example.com",
      "insert 22 user22 person22@example.com",
      "insert 53 user53 person53@example.com",
      "insert 51 user51 person51@example.com",
      "insert 59 user59 person59@example.com",
      "insert 32 user32 person32@example.com",
      "insert 36 user36 person36@example.com",
      "insert 79 user79 person79@example.com",
      "insert 10 user10 person10@example.com",
      "insert 33 user33 person33@example.com",
      "insert 20 user20 person20@example.com",
      "insert 4 user4 person4@example.com",
      "insert 35 user35 person35@example.com",
      "insert 76 user76 person76@example.com",
      "insert 49 user49 person49@example.com",
      "insert 24 user24 person24@example.com",
      "insert 70 user70 person70@example.com",
      "insert 48 user48 person48@example.com",
      "insert 39 user39 person39@example.com",
      "insert 15 user15 person15@example.com",
      "insert 47 user47 person47@example.com",
      "insert 30 user30 person30@example.com",
      "insert 86 user86 person86@example.com",
      "insert 31 user31 person31@example.com",
      "insert 68 user68 person68@example.com",
      "insert 37 user37 person37@example.com",
      "insert 66 user66 person66@example.com",
      "insert 63 user63 person63@example.com",
      "insert 40 user40 person40@example.com",
      "insert 78 user78 person78@example.com",
      "insert 19 user19 person19@example.com",
      "insert 46 user46 person46@example.com",
      "insert 14 user14 person14@example.com",
      "insert 81 user81 person81@example.com",
      "insert 72 user72 person72@example.com",
      "insert 6 user6 person6@example.com",
      "insert 50 user50 person50@example.com",
      "insert 85 user85 person85@example.com",
      "insert 67 user67 person67@example.com",
      "insert 2 user2 person2@example.com",
      "insert 55 user55 person55@example.com",
      "insert 69 user69 person69@example.com",
      "insert 5 user5 person5@example.com",
      "insert 65 user65 person65@example.com",
      "insert 52 user52 person52@example.com",
      "insert 1 user1 person1@example.com",
      "insert 29 user29 person29@example.com",
      "insert 9 user9 person9@example.com",
      "insert 43 user43 person43@example.com",
      "insert 75 user75 person75@example.com",
      "insert 21 user21 person21@example.com",
      "insert 82 user82 person82@example.com",
      "insert 12 user12 person12@example.com",
      "insert 18 user18 person18@example.com",
      "insert 60 user60 person60@example.com",
      "insert 44 user44 person44@example.com",
      ".btree",
      ".exit\n",
    ];
    const commandsExpectedResult = [
      "lyt-db> tree:",
      "- internal (size 1)",
      " - internal (size 2)",
      "  - leaf (size 7)",
      "   - 1",
      "   - 2",
      "   - 4",
      "   - 5",
      "   - 6",
      "   - 7",
      "   - 8",
      "  - key 8",
      "  - leaf (size 11)",
      "   - 9",
      "   - 10",
      "   - 12",
      "   - 13",
      "   - 14",
      "   - 15",
      "   - 18",
      "   - 19",
      "   - 20",
      "   - 21",
      "   - 22",
      "  - key 22",
      "  - leaf (size 8)",
      "   - 24",
      "   - 25",
      "   - 29",
      "   - 30",
      "   - 31",
      "   - 32",
      "   - 33",
      "   - 35",
      " - key 35",
      " - internal (size 3)",
      "  - leaf (size 12)",
      "   - 36",
      "   - 37",
      "   - 39",
      "   - 40",
      "   - 43",
      "   - 44",
      "   - 46",
      "   - 47",
      "   - 48",
      "   - 49",
      "   - 50",
      "   - 51",
      "  - key 51",
      "  - leaf (size 11)",
      "   - 52",
      "   - 53",
      "   - 54",
      "   - 55",
      "   - 56",
      "   - 58",
      "   - 59",
      "   - 60",
      "   - 63",
      "   - 65",
      "   - 66",
      "  - key 66",
      "  - leaf (size 7)",
      "   - 67",
      "   - 68",
      "   - 69",
      "   - 70",
      "   - 71",
      "   - 72",
      "   - 75",
      "  - key 75",
      "  - leaf (size 8)",
      "   - 76",
      "   - 77",
      "   - 78",
      "   - 79",
      "   - 81",
      "   - 82",
      "   - 85",
      "   - 86",
      "lyt-db> ",
    ];

    const result = runScript(commands).slice(64);
    expect(result).toStrictEqual(commandsExpectedResult);
  });
});
