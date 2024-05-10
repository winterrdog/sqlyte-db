const { execSync } = require("child_process");

describe("database", function () {
  const runScript = (commands) => {
    const childProcess = execSync("./sqlyte", { input: commands.join("\n") });
    const rawOutput = childProcess.toString();
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
      result.push(".exit\n");
      return result;
    };
    const commandsExpectedResult = "lyt-db> error: table's full.";
    const result = runScript(createManyEntries());
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
});
