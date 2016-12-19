set term pdf font "Helvetica, 10"

# some line types with different colors, you can use them by using line styles in the plot command afterwards (linestyle X)
set style line 1 lt 1 lc rgb "#FF0000" lw 7 # red
set style line 2 lt 1 lc rgb "#00FF00" lw 7 # green
set style line 3 lt 1 lc rgb "#0000FF" lw 7 # blue
set style line 4 lt 1 lc rgb "#000000" lw 7 # black
set style line 5 lt 1 lc rgb "#CD00CD" lw 7 # purple
set style line 7 lt 3 lc rgb "#000000" lw 7 # black, dashed line

set output "notOptimal.pdf"
set title "Average number of not optimal successors"

# indicates the labels
set ylabel "Average No."
set xlabel "Cycles"

# set the grid on
set grid x,y

# set the key, options are top/bottom and left/right
set key top right

# indicates the ranges
set yrange [0:] # example of a closed range (points outside will not be displayed)
set xrange [0:] # example of a range closed on one side only, the max will determined automatically

plot "not_optimal/not_optimal_churn.parse" u ($1):($2) with lines linestyle 1 title "churn",\
     "not_optimal/not_optimal_massive_churn.parse" u ($1):($2) with lines linestyle 2 title "massive churn",\
     "not_optimal/not_optimal_no_churn.parse" u ($1):($2) with lines linestyle 3 title "no churn"

# $1 is column 1. You can do arithmetics on the values of the columns
