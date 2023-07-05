package root

import (
	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "bitcask",
	Short: "Less memory consumption, Larger storage capacity, and almost Constant read performance",
	Long:  `bitcask's goal is to compromise between performance and storage costs, as an alternative to Redis in some scenarios.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() {
	rootCmd.Execute()
}

func AddCommands(cmds ...*cobra.Command) {
	rootCmd.AddCommand(cmds...)
}

//func init() {
//	cobra.OnInitialize(initConfig)
//
//	// Here you will define your flags and configuration settings.
//	// Cobra supports persistent flags, which, if defined here,
//	// will be global for your application.
//
//	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.demo.yaml)")
//
//	// Cobra also supports local flags, which will only run
//	// when this action is called directly.
//}
//
//// initConfig reads in config file and ENV variables if set.
//func initConfig() {
//	if cfgFile != "" {
//		// Use config file from the flag.
//		viper.SetConfigFile(cfgFile)
//	} else {
//		// Find home directory.
//		home, err := homedir.Dir()
//		if err != nil {
//			fmt.Println(err)
//			os.Exit(1)
//		}
//
//		// Search config in home directory with name ".demo" (without extension).
//		viper.AddConfigPath(home)
//		viper.SetConfigName(".demo")
//	}
//
//	viper.AutomaticEnv() // read in environment variables that match
//
//	// If a config file is found, read it in.
//	if err := viper.ReadInConfig(); err == nil {
//		fmt.Println("Using config file:", viper.ConfigFileUsed())
//	}
//}
