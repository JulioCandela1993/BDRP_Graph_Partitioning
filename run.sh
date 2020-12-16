declare -a algs=('InitializeSampleRD' 'InitializeSampleHD') # 'InitializeSampleGD' 'BGRAP') 
#declare -a algs=('InitializeSampleHD')
#declare -a files=(Twitter)
#declare -a files=(sk-2005) --> todos 3 its = 8h
#declare -a files=(Pokec soc-LiveJournal Orkut graph500-scale23-ef16) --> todos algs, 5 its = 9h
#declare -a files=(WikiTalk BerkeleyStan Flixster DelaunaySC) -->todos algs, 5 its = 5h
declare -a files=(WikiTalk BerkeleyStan Flixster DelaunaySC Pokec soc-LiveJournal Orkut graph500-scale23-ef16 sk-2005 Twitter)

declare -a betas=(0.15)
declare -a sigmas=(0.015)
declare -a taus=(2)
declare -a partitions=(4)
declare save_dd=false

for graph in "${files[@]}"
do
        for sampling in "${algs[@]}"
        do
                for beta in "${betas[@]}"
                do
                        for sigma in "${sigmas[@]}"
                        do
                                for tau in "${taus[@]}"
                                do
                                        for partition in "${partitions[@]}"
                                        do
                                                for i in {1..1}
                                                do
                                                        make run_optimized sampling=$sampling graph=$graph partitions=$partition beta=$beta sigma=$sigma tau=$tau save_dd=$save_dd
                                                        wait $!
                                                        echo "-" $graph $sampling >> RUNNING
                                                done
                                        done
                                done
                        done
                done
        done
done

echo "-----">> FINISHED
echo ${algs[@]} >> FINISHED
echo ${files[@]} >> FINISHED
echo ${partitions[@]} >> FINISHED
echo "-----">> FINISHED

make get_results
wait $!

echo "Saved" >> FINISHED
rm RUNNING