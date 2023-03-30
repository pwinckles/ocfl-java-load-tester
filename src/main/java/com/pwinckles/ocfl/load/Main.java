package com.pwinckles.ocfl.load;

import picocli.CommandLine;

public class Main {

    public static void main(String[] args) {
        System.exit(new CommandLine(new NewObjectLoadTestCmd()).execute(args));
    }
}
